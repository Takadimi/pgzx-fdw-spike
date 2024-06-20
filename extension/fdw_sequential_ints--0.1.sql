\echo Use "CREATE EXTENSION fdw_sequential_ints " to load this file. \quit

CREATE FUNCTION fdw_sequential_ints_handler() RETURNS fdw_handler
AS '$libdir/fdw_sequential_ints'
LANGUAGE C IMMUTABLE;

CREATE FOREIGN DATA WRAPPER fdw_sequential_ints HANDLER fdw_sequential_ints_handler;
