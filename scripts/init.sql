CREATE TABLE "users" (
  "id" SERIAL UNIQUE PRIMARY KEY,
  "summonername" varchar,
  "summonerid" varchar UNIQUE,
  "league" varchar,
  "ranktier" varchar,
  "puuid" varchar,
  "leaguepoints" int,
  "region" varchar,
  "updateddate" timestamp DEFAULT (now())
);

CREATE TABLE "matchinfo" (
  "id" SERIAL UNIQUE PRIMARY KEY,
  "matchid" varchar,
  "puuid" varchar,
  "matchdate" timestamp DEFAULT (now()),
  "updateddate" timestamp DEFAULT (now()),
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

CREATE TABLE "matchtraits" (
  "matchinfoid" int,
  "name" varchar,
  "numberunits" int,
  "tier" int
);

ALTER TABLE "matchunits" ADD FOREIGN KEY ("matchinfoid") REFERENCES "matchinfo" ("id");

ALTER TABLE "matchtraits" ADD FOREIGN KEY ("matchinfoid") REFERENCES "matchinfo" ("id");
