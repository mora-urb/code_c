#include <regex.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>

#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>

#define N_CHILDREN 61
#define BUF_SIZE   8192
#define FILE_NAME  "text_to_read.txt"
#define PATRON "existe"

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
            printf("waitpid fallo\n");
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
    int en_parrafo = 0;   // Semáforo de si estamos en párrafo
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

static void loop_hijo(int idx_hijo, int data_fd, int r_from_parent, int w_to_parent) {
    char local[BUF_SIZE];
    size_t used = 0;

    for (;;) {
        char cmd[64];
        int rd = read(r_from_parent, cmd, sizeof(cmd)-1);
        if (rd <= 0) _exit(2);
        cmd[rd] = '\0';

        if (cmd[0] == 'E') {
            _exit(0);

        } else if (cmd[0] == 'A') {
            long long off_ll = 0;
            if (sscanf(cmd, "A %lld", &off_ll) == 1) {
                off_t start = (off_t)off_ll;
                off_t next;

                used = leer_parrafos_hasta_llenar(data_fd, start, local, BUF_SIZE, &next);

                (void)trabajo_hijos_buffer(local, used);

                dprintf(w_to_parent, "NEXT %lld\n", (long long)next);

                used = 0;
            }
        }
    }
}

int main() {
    double time_spent;
    struct timespec t0, t1;

    int data_fd = open(FILE_NAME, O_RDONLY);
    if (data_fd < 0) { fputs("no se pudo abrir archivo\n", stderr); return 1; }

    struct stat st;
    if (fstat(data_fd, &st) != 0) { fputs("fstat fallo\n", stderr); return 1; }
    off_t fsize = st.st_size;

    int to_child[N_CHILDREN][2];
    int from_child[N_CHILDREN][2];
    for (int i = 0; i < N_CHILDREN; i++) {
        if (pipe(to_child[i]) != 0 || pipe(from_child[i]) != 0) {
            fputs("pipe fallo\n", stderr); return 1;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t0);

    pid_t pid = -1;
    int soy_hijo = 0;
    int mi_idx = -1;

    for (int i = 0; i < N_CHILDREN; i++) {
        pid = fork();
        if (pid < 0) { perror("fork"); exit(1); }
        if (pid == 0) { // HIJO
            soy_hijo = 1;
            mi_idx = i;
            break;
        }
    }

    if (!soy_hijo) {
        for (int i = 0; i < N_CHILDREN; i++) {
            close(to_child[i][0]);
            close(from_child[i][1]);
        }

        off_t next_off = 0;
        int turno = 0;

        while (next_off < fsize) {
            int i = turno % N_CHILDREN;

            dprintf(to_child[i][1], "A %lld\n", (long long)next_off);

            char resp[64];
            int rd = read(from_child[i][0], resp, sizeof(resp)-1);
            if (rd <= 0) break;
            resp[rd] = '\0';

            long long nx = (long long)next_off;
            if (sscanf(resp, "NEXT %lld", &nx) == 1) {
                if ((off_t)nx > next_off) {
                    next_off = (off_t)nx;
                }
            }
            turno++;
        }

        for (int i = 0; i < N_CHILDREN; i++) {
            write(to_child[i][1], "E\n", 2);
        }
        for (int i = 0; i < N_CHILDREN; i++) {
            close(to_child[i][1]);
        }

        espera_padre();

        clock_gettime(CLOCK_MONOTONIC, &t1);
        time_spent = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
        printf("Tiempo de ejecucion (padre esperando a %d hijos): %.6f s\n",
               N_CHILDREN, time_spent);

        close(data_fd);
        return 0;

    } else {
        printf("Process ID: %ld, Parent ID: %ld, idx=%d\n",
               (long)getpid(), (long)getppid(), mi_idx);

        for (int k = 0; k < N_CHILDREN; k++) {
            close(to_child[k][1]);
            close(from_child[k][0]);
            if (k != mi_idx) {
                close(to_child[k][0]);
                close(from_child[k][1]);
            }
        }

        loop_hijo(mi_idx, data_fd, to_child[mi_idx][0], from_child[mi_idx][1]);
    }
}
