package pgdriver

// The following sql queries should not be modified, only added.
// A developer should create a migrate file with queries listed below.

var SQL001 = `
CREATE SEQUENCE IF NOT EXISTS leases_rev_seq;

CREATE TABLE IF NOT EXISTS leases
(
   resource text NOT NULL PRIMARY KEY,
   rev bigint NOT NULL,
   owner text NOT NULL,
   expires_at timestamp(0) NOT NULL
);
`

var SQL002 = `
CREATE INDEX IF NOT EXISTS leases_resource_expires_at_idx ON leases (resource, expires_at);
CREATE INDEX IF NOT EXISTS leases_resource_owner_idx ON leases (resource, owner);
`

var SQL003 = `
ALTER TABLE leases SET UNLOGGED;
`

var Migrations = []string{
	SQL001,
	SQL002,
	SQL003,
}
