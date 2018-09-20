PRAGMA foreign_keys=ON;
ATTACH 'db/fcc.sqlite' AS fcc;
ATTACH 'db/app-fcc.sqlite' AS app;
.read parse/queries/inactive-vanity.sql
