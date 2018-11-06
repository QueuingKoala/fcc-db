PRAGMA foreign_keys=ON;
ATTACH 'fcc.sqlite' AS fcc;
ATTACH 'app-fcc.sqlite' AS app;
.read parse/queries/inactive-vanity.sql
