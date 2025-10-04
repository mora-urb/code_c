#include <regex.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <time.h>

#define N_CHILDREN 10
#define BUF_SIZE   8192
#define FILE_NAME  "text_to_read.txt"
#define PATRON     "existe"

#define MSGSZ 128
struct message {
  long type;
  char text[MSGSZ];
};

static size_t cstrlen(const char *s) {
    size_t n = 0;
    while (s[n] != '\0') n++;
    return n;
}

static int match_at(const char *buf, size_t len, size_t pos, const char *word, size_t wlen) {
    if (pos + wlen > len) return 0;
    for (size_t k = 0; k < wlen; k++) {
        if (buf[pos + k] != word[k]) return 0;
    }
    return 1;
}

int print_word_context_100(const char *buf, size_t len, const char *word) {
    const size_t CTX = 100;
    size_t wlen = cstrlen(word);
    int matches = 0;

    if (wlen == 0) return 0;
    if (len == 0) return 0;

    for (size_t i = 0; i + wlen <= len; i++) {
        if (!match_at(buf, len, i, word, wlen)) continue;

        size_t start = (i > CTX) ? (i - CTX) : 0;
        size_t end   = i + wlen + CTX;
        if (end > len) end = len;

        fwrite(buf + start, 1, end - start, stdout);
        fputc('\n', stdout);

        matches++;
    }

    return matches;
}

static int trabajo_hijos_buffer(const char *buf, size_t len) {
    regex_t regex;
    int rc = 0;
    int reti;

    char local[BUF_SIZE + 1];

    size_t copy = (len < BUF_SIZE) ? len : BUF_SIZE;

    for (size_t i = 0; i < copy; i++) local[i] = buf[i];
    local[copy] = '\0';

    reti = regcomp(&regex, PATRON, REG_EXTENDED | REG_NOSUB);
    if (reti != 0) {
        fprintf(stderr, "El patrón no se pudo compilar\n");
        return 2;
    }

    reti = regexec(&regex, local, 0, NULL, 0);
    if (reti == 0) {
        puts("Match - start");
        print_word_context_100(local, copy, PATRON);
        puts("Match - end");
        rc = 0;
    } else if (reti == REG_NOMATCH) {
        puts("No match");
        rc = 1;
    } else {
        fprintf(stderr, "Regex match falló (no NOMATCH)\n");
        rc = 2;
    }

    regfree(&regex);
    return rc;
}

static void espera_padre(void){
    int status, terminados = 0;

    while (terminados < N_CHILDREN) {
        pid_t child = waitpid(-1, &status, 0);
        if (child == -1) {
            perror("waitpid fallo");
            break;
        }

        if (WIFEXITED(status)) {
            printf("Final de hijo: %ld (exit=%d)\n",
                   (long)child, WEXITSTATUS(status));
        } 
        else if (WIFSIGNALED(status)) {
            printf("Hijo %ld terminó por señal %d\n",
                   (long)child, WTERMSIG(status));
        }
        terminados++;
    }
}

static size_t leer_parrafos_hasta_llenar(int data_fd, off_t start_off, char *dst, size_t cap, off_t *next_off) {
    size_t used = 0;
    off_t cur = start_off;
    int en_parrafo = 0;
    int nl_streak = 0;

    for (;;) {
        char ch;
        ssize_t r = pread(data_fd, &ch, 1, cur);
        if (r != 1) { *next_off = cur; break; }

        if (used + 1 > cap) { *next_off = cur; break; }

        if (!en_parrafo) { en_parrafo = 1; nl_streak = 0; }

        dst[used++] = ch;
        cur++;

        if (ch == '\n') {
            nl_streak++;
            if (nl_streak >= 2) {
                en_parrafo = 0;
                nl_streak = 0;
            }
        } else {
            nl_streak = 0;
        }
    }

    *next_off = cur;
    return used;
}

/* ---------- Cambio: hijo mide duración y la devuelve en el mensaje ---------- */
static void loop_hijo(int idx_hijo, int data_fd, int q_p2c, int q_c2p) {
    char local[BUF_SIZE];
    size_t used = 0;

    const long mytype = (long)(idx_hijo + 1);
    struct message msg;

    for (;;) {
        if (msgrcv(q_p2c, &msg, MSGSZ, mytype, 0) < 0) {
            perror("msgrcv hijo");
            _exit(2);
        }

        msg.text[MSGSZ - 1] = '\0';
        if (msg.text[0] == 'E') {
            _exit(0);
        } else if (msg.text[0] == 'A') {
            long long off_ll = 0;
            if (sscanf(msg.text, "A %lld", &off_ll) == 1) {
                off_t start = (off_t)off_ll;
                off_t next;

                /* medir tiempo antes de leer+procesar */
                struct timespec t0, t1;
                clock_gettime(CLOCK_MONOTONIC, &t0);

                used = leer_parrafos_hasta_llenar(data_fd, start, local, BUF_SIZE, &next);

                (void)trabajo_hijos_buffer(local, used);

                clock_gettime(CLOCK_MONOTONIC, &t1);
                double duration_s = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;

                struct message resp;
                resp.type = mytype;

                /* Enviamos NEXT <next_off> DUR <duration_s> */
                snprintf(resp.text, MSGSZ, "NEXT %lld DUR %.6f", (long long)next, duration_s);

                if (msgsnd(q_c2p, &resp, MSGSZ, 0) < 0) {
                    perror("msgsnd hijo->padre");
                    _exit(3);
                }

                used = 0;
            }
        }
    }
}

/* -------------------------------------------------------------------------- */

int main() {
    double time_spent;
    struct timespec t0, t1;

    int data_fd = open(FILE_NAME, O_RDONLY);
    if (data_fd < 0) { fputs("no se pudo abrir archivo\n", stderr); return 1; }

    struct stat st;
    if (fstat(data_fd, &st) != 0) { fputs("fstat fallo\n", stderr); return 1; }
    off_t fsize = st.st_size;

    int q_p2c = msgget(IPC_PRIVATE, IPC_CREAT | S_IRUSR | S_IWUSR);
    if (q_p2c < 0) { perror("msgget q_p2c"); return 1; }

    int q_c2p = msgget(IPC_PRIVATE, IPC_CREAT | S_IRUSR | S_IWUSR);
    if (q_c2p < 0) { perror("msgget q_c2p"); msgctl(q_p2c, IPC_RMID, NULL); return 1; }

    /* ---------- Cambio: abrir CSV para registro en el proceso padre ---------- */
    char logfile_name[128];
    snprintf(logfile_name, sizeof(logfile_name), "log_%d.csv", N_CHILDREN);
    FILE *logf = fopen(logfile_name, "w");
    if (!logf) {
        perror("fopen logfile");
        /* seguimos pero sin logfile */
    } else {
        /* cabecera CSV */
        fprintf(logf, "start_off,next_off,child_idx,child_pid,duration_s,timestamp\n");
        fflush(logf);
    }

    clock_gettime(CLOCK_MONOTONIC, &t0);

    pid_t pid = -1;
    int soy_hijo = 0;
    int mi_idx = -1;

    pid_t child_pids[N_CHILDREN];
    for (int i = 0; i < N_CHILDREN; i++) child_pids[i] = -1;

    for (int i = 0; i < N_CHILDREN; i++) {
        pid = fork();
        if (pid < 0) { perror("fork"); exit(1); }
        if (pid == 0) { // HIJO
            soy_hijo = 1;
            mi_idx = i;
            break;
        } else {
            /* padre guarda pid del hijo en el índice i */
            child_pids[i] = pid;
        }
    }

    if (!soy_hijo) {
        off_t next_off = 0;
        int turno = 0;

        while (next_off < fsize) {
            int i = turno % N_CHILDREN;

            struct message cmd;
            cmd.type = (long)(i + 1);
            /* Guardamos el offset que enviamos para luego registrar start_off */
            off_t sent_off = next_off;
            snprintf(cmd.text, MSGSZ, "A %lld", (long long)sent_off);
            if (msgsnd(q_p2c, &cmd, MSGSZ, 0) < 0) {
                perror("msgsnd padre->hijo");
                break;
            }

            struct message resp;
            if (msgrcv(q_c2p, &resp, MSGSZ, (long)(i + 1), 0) < 0) {
                perror("msgrcv padre<-hijo");
                break;
            }

            resp.text[MSGSZ - 1] = '\0';

            long long nx = (long long)next_off;
            double dur = 0.0;
            if (sscanf(resp.text, "NEXT %lld DUR %lf", &nx, &dur) == 2) {
                if ((off_t)nx > next_off) {
                    next_off = (off_t)nx;
                }
            } else if (sscanf(resp.text, "NEXT %lld", &nx) == 1) {
                if ((off_t)nx > next_off) {
                    next_off = (off_t)nx;
                }
            }

            /* Escribir en CSV: start_off,next_off,child_idx,child_pid,duration_s,timestamp */
            if (logf) {
                time_t now = time(NULL);
                char timestr[64];
                struct tm tm_now;
                localtime_r(&now, &tm_now);
                strftime(timestr, sizeof(timestr), "%Y-%m-%d %H:%M:%S", &tm_now);

                int child_idx = (int)(resp.type - 1);
                pid_t child_pid = -1;
                if (child_idx >= 0 && child_idx < N_CHILDREN) child_pid = child_pids[child_idx];

                fprintf(logf, "%lld,%lld,%d,%ld,%.6f,%s\n",
                        (long long)sent_off, nx, child_idx, (long)child_pid, dur, timestr);
                fflush(logf);
            }

            turno++;
        }

        for (int i = 0; i < N_CHILDREN; i++) {
            struct message fin;
            fin.type = (long)(i + 1);
            strcpy(fin.text, "E");

            if (msgsnd(q_p2c, &fin, MSGSZ, 0) < 0) {
                perror("msgsnd fin");
            }
        }

        espera_padre();

        clock_gettime(CLOCK_MONOTONIC, &t1);

        time_spent = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
        printf("Tiempo de ejecucion (padre esperando a %d hijos): %.6f s\n",
               N_CHILDREN, time_spent);

        if (msgctl(q_p2c, IPC_RMID, NULL) < 0) perror("msgctl IPC_RMID q_p2c");
        if (msgctl(q_c2p, IPC_RMID, NULL) < 0) perror("msgctl IPC_RMID q_c2p");

        if (logf) fclose(logf);
        close(data_fd);
        return 0;

    } else {
        printf("Process ID: %ld, Parent ID: %ld, idx=%d\n",
               (long)getpid(), (long)getppid(), mi_idx);

        loop_hijo(mi_idx, data_fd, q_p2c, q_c2p);
    }
}
