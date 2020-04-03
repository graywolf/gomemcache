#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <libmemcached/memcached.h>

static memcached_return_t rc;
static memcached_st *mc;

static void
die(const char *fmt, ...) {
	va_list args;
	va_start(args, fmt);

	vfprintf(stderr, fmt, args);
	fputs("\n", stderr);

	va_end(args);
	exit(EXIT_FAILURE);
}

static void
mc_ensure(const char *fmt, ...) {
	if (rc != MEMCACHED_SUCCESS) {
		fprintf(stderr, "fail:\n");

		va_list ap;
		va_start(ap, fmt);
		vfprintf(stderr, fmt, ap);
		va_end(ap);

		fputs("\n", stderr);

		die("memcached: %s", memcached_strerror(mc, rc));
	}
}

static void
process_server_line(char *line) {
	switch (*(line++)) {
	case 't': {
		const char *addr = NULL;
		const char *port = NULL;

		for (; *line == ' '; line++) ;
		addr = line;
		for (; *line != ' '; line++) ;
		*(line++) = 0;

		for (; *line == ' '; line++) ;
		port = line;
		for (; *line != ' ' && *line != '\n'; line++) ;
		*(line++) = 0;

		rc = memcached_server_add_with_weight( mc, addr, atoi(port), 1);
		mc_ensure("add tcp server: %s:%d", addr, atoi(port));
		break;
	}
	case 'u': {
		const char *path = NULL;

		for (; *line == ' '; line++) ;
		path = line;
		for (; *line != ' ' && *line != '\n'; line++) ;
		*(line++) = 0;

		rc = memcached_server_add_unix_socket_with_weight( mc, path, 1);
		mc_ensure("add server unix: %s", path);
		break;
	}
	default:
		die("Unexpected server type: %c", *line);
		break;
	}
}

static void
process_data_line(char *key) {
	size_t key_len = strlen(key);
	size_t val_len = 0;
	uint32_t flags = 0;

	key[key_len-- - 1] = 0;

	char *val = memcached_get(mc, key, key_len, &val_len, &flags, &rc);
	if (val == NULL) {
		if (rc == MEMCACHED_NOTFOUND) {
			die("Key not found: %s", key);
		} else {
			mc_ensure("get `%s'", key);
		}
	}

	if (val_len != 74) {
		die("val_len is expected to be 74 bytes, got: %llu", val_len);
	}
	if (
		   (memcmp("value :-> ", val     , 10) != 0)
		|| (memcmp(key,          val + 10, 64) != 0)
	) {
		die("value does not match: `%s'", val);
	}
	free(val);
}

int
main(int argc, char **argv) {
	char *line = NULL;
	size_t line_n = 0;
	ssize_t read;

	if (argc != 3) {
		die("Usage: test-c SERVERS DATA");
	}

	mc = memcached_create(NULL);
	if (mc == NULL) {
		die("Cannot create memcached: %s", strerror(errno));
	}

	rc = memcached_behavior_set(mc, MEMCACHED_BEHAVIOR_KETAMA_WEIGHTED, 1);
	mc_ensure("set ketama");

	FILE *sf = fopen(argv[1], "r");
	if (sf == NULL) {
		die("Cannot open servers file: %s", strerror(errno));
	}

	FILE *df = fopen(argv[2], "r");
	if (df == NULL) {
		die("Cannot open data file: %s", strerror(errno));
	}

	while ((read = getline(&line, &line_n, sf)) != -1) {
		process_server_line(line);
	}
	if (ferror(sf)) {
		die("Error while reading servers file: %s", strerror(errno));
	}

	while ((read = getline(&line, &line_n, df)) != -1) {
		process_data_line(line);
	}
	if (ferror(df)) {
		die("Error while reading data file: %s", strerror(errno));
	}

	return EXIT_SUCCESS;
}
