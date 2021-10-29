CREATE TABLE "users" (
  "id" SERIAL UNIQUE PRIMARY KEY,
  "summonername" varchar,
  "summonerid" varchar UNIQUE,
  "rankedleague" varchar,
  "puuid" varchar,
  "leaguepoints" int,
  "region" varchar
);

CREATE TABLE "matchinfo" (
  "id" SERIAL UNIQUE PRIMARY KEY,
  "matchid" varchar,
  "userid" int,
  "placement" int
);

CREATE TABLE "matchunits" (
  "matchinfoid" int,
  "unitname" varchar,
  "unitstar" int,
  "item1" int,
  "item2" int,
  "item3" int
);

ALTER TABLE "matchinfo" ADD FOREIGN KEY ("userid") REFERENCES "users" ("id");

ALTER TABLE "matchunits" ADD FOREIGN KEY ("matchinfoid") REFERENCES "matchinfo" ("id");