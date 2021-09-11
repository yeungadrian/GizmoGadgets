// TFT Table Design

Table Users as u {
  id int [pk, increment]
  name varchar
  summonerId varchar
  rankedLeagueId int [ref: < rl.id]
  puuid varchar
  leaguepoints int
  regionId int [ref:< r.id]
}

Table Region as r {
  id int [pk, increment]
  regionName varchar
}

Table RankedLeague as rl {
  id int [pk, increment]
  name varchar
  tier varchar
}

Table Matches as m {
  id int [pk, increment]
  matchId varchar
  date timestamp
  uploadedAt timestamp
  region varchar
}


Table MatchInfo as mi {
  id int [pk, increment]
  matchId int [ref: < m.id]
  matchData varchar
  puuid varchar [ref: < u.puuid]
  regionId int [ref: < r.id]
  placement int
}

Table SetFivetraits as st {
  id int [pk, increment]
  traitName varchar
  tierTotal int
}

Table SetFiveunits as su {
  id int [pk, increment]
  characterId varchar
  cost int
  trait1 int
  trait2 int
  trait3 int
}

Table MatchTraits{
   matchInfoId int [ref: < mi.id]
   traitName varchar
   numUnits int
   currentTier int
}

Table MatchUnits{
   matchInfoId int [ref: < mi.id]
   UnitName varchar
   unitStar int
   item1 int
   item2 int
   item3 int
}