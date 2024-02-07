with import <nixpkgs> {};

mkShell {
  buildInputs = [ postgresql_16 ];

  shellHook = ''
    pushtrap () {
      test "$traps" || trap 'set +eu; eval $traps' 0;
      traps="$*; $traps"
    }

    TMP=$(mktemp -d)
    DB=$TMP/db
    SOCKET=$TMP

    initdb -D $DB
    pg_ctl -D $DB \
      -l $TMP/logfile \
      -o "--unix_socket_directories='$SOCKET'" \
      -o "--listen_addresses=''\'''\'" \
      start

    createdb -h $SOCKET log
    export DATABASE_URL="postgresql:///log?host=$SOCKET"

    pushtrap "pg_ctl -D $DB stop; rm -rf $TMP" EXIT
  '';
}
