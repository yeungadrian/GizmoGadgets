CREATE TABLE "users" (
  "id" SERIAL PRIMARY KEY,
  "summonername" varchar,
  "summonerid" varchar,
  "rankedleague" varchar,
  "puuid" varchar,
  "leaguepoints" int,
  "region" varchar
);

CREATE TABLE "matchdata" (
  "id" SERIAL PRIMARY KEY,
  "matchid" varchar,
  "date" timestamp,
  "uploadedat" timestamp,
  "region" varchar
);

CREATE TABLE "matchuserinfo" (
  "id" SERIAL PRIMARY KEY,
  "matchid" int,
  "matchdata" varchar,
  "userid" int,
  "region" varchar,
  "placement" int
);

CREATE TABLE "matchtraits" (
  "matchinfoid" int,
  "traitname" varchar,
  "numunits" int,
  "currenttier" int
);

CREATE TABLE "matchunits" (
  "matchinfoid" int,
  "unitname" varchar,
  "unitstar" int,
  "item1" int,
  "item2" int,
  "item3" int
);

ALTER TABLE "matchuserinfo" ADD FOREIGN KEY ("matchid") REFERENCES "matchdata" ("id");

ALTER TABLE "matchuserinfo" ADD FOREIGN KEY ("userid") REFERENCES "users" ("id");

ALTER TABLE "matchtraits" ADD FOREIGN KEY ("matchinfoid") REFERENCES "matchuserinfo" ("id");

ALTER TABLE "matchunits" ADD FOREIGN KEY ("matchinfoid") REFERENCES "matchuserinfo" ("id");
