// GDS#01: PageRank zemalja na osnovu raspodele medalja
CALL gds.graph.drop('countrySharedMedals', false) YIELD graphName
RETURN graphName;

CALL gds.graph.project.cypher(
  'countrySharedMedals',
  'MATCH (c:Country) RETURN id(c) AS id',
  '
    MATCH (c1:Country)<-[:REPRESENTS]-(entity1)-[:WON_MEDAL]->(e:Event)<-[:WON_MEDAL]-(entity2)-[:REPRESENTS]->(c2:Country)
    WHERE id(c1) < id(c2)
    WITH id(c1) AS c1Id, id(c2) AS c2Id, count(DISTINCT e) AS weight
    UNWIND [[c1Id, c2Id], [c2Id, c1Id]] AS pair
    RETURN pair[0] AS source,
           pair[1] AS target,
           weight AS weight
  ',
  {validateRelationships: false}
);

CALL gds.pageRank.stream('countrySharedMedals', {relationshipWeightProperty: 'weight', dampingFactor: 0.85, maxIterations: 40})
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).name AS country, round(score, 4) AS influence_score
ORDER BY influence_score DESC
LIMIT 20;

CALL gds.graph.drop('countrySharedMedals') YIELD graphName
RETURN graphName;


// GDS#02: Similarity između sportista iz država bivše federacije na osnovu timova i protiv koga su igrali
CALL gds.graph.drop('athleteRegionalSimilarity', false) YIELD graphName
RETURN graphName;

CALL gds.graph.project.cypher(
  'athleteRegionalSimilarity',
  '
    WITH ["slovenia", "bosnia & herzegovina", "serbia", "croatia", "kosovo", "north macedonia"] AS focusCountries
    MATCH (a:Athlete)-[:REPRESENTS]->(c:Country)
    WHERE toLower(c.name) IN focusCountries
    RETURN id(a) AS id
  ',
  '
    WITH ["slovenia", "bosnia & herzegovina", "serbia", "croatia", "kosovo", "north macedonia"] AS focusCountries
    MATCH (a1:Athlete)-[:REPRESENTS]->(c1:Country)
    WHERE toLower(c1.name) IN focusCountries
    MATCH (a2:Athlete)-[:REPRESENTS]->(c2:Country)
    WHERE toLower(c2.name) IN focusCountries AND id(a1) <> id(a2)
    OPTIONAL MATCH (a1)-[:MEMBER_OF]->(t:Team)<-[:MEMBER_OF]-(a2)
    WITH a1, a2, count(DISTINCT t) AS sharedTeams
    OPTIONAL MATCH (a1)-[:COMPETED_IN]->(e:Event)<-[:COMPETED_IN]-(a2)
    WITH a1,
         a2,
         sharedTeams,
         count(DISTINCT e) AS sharedEvents
    WITH id(a1) AS a1Id,
         id(a2) AS a2Id,
         sharedTeams,
         sharedEvents
    WITH a1Id,
         a2Id,
         sharedTeams,
         sharedEvents,
         (sharedTeams + sharedEvents) AS combinedWeight
    WHERE combinedWeight > 0
    UNWIND [[a1Id, a2Id], [a2Id, a1Id]] AS pair
    RETURN pair[0] AS source,
           pair[1] AS target,
           combinedWeight AS weight
  ',
  {validateRelationships: false}
);

CALL gds.nodeSimilarity.stream('athleteRegionalSimilarity', {similarityCutoff: 0.2, degreeCutoff: 2, relationshipWeightProperty: 'weight'})
YIELD node1, node2, similarity
WITH gds.util.asNode(node1) AS athleteOne,
     gds.util.asNode(node2) AS athleteTwo,
     similarity
MATCH (athleteOne)-[:REPRESENTS]->(c1:Country)
MATCH (athleteTwo)-[:REPRESENTS]->(c2:Country)
WHERE toLower(c1.name) IN ["slovenia", "bosnia & herzegovina", "serbia", "croatia", "kosovo", "north macedonia"]
  AND toLower(c2.name) IN ["slovenia", "bosnia & herzegovina", "serbia", "croatia", "kosovo", "north macedonia"]
  AND c1.name <> c2.name
RETURN athleteOne.name AS athlete_one,
       c1.name AS country_one,
       athleteTwo.name AS athlete_two,
       c2.name AS country_two,
       round(similarity, 2) AS similarity
ORDER BY similarity DESC;

CALL gds.graph.drop('athleteRegionalSimilarity') YIELD graphName
RETURN graphName;


// GDS#03: Louvain detekcija klastera zemalja i medalja
CALL gds.graph.drop('countryMedalCommunities', false) YIELD graphName
RETURN graphName;

CALL gds.graph.project.cypher(
  'countryMedalCommunities',
  'MATCH (c:Country) RETURN id(c) AS id',
  '
    MATCH (c1:Country)<-[:REPRESENTS]-(entity1)-[:WON_MEDAL]->(e:Event)<-[:WON_MEDAL]-(entity2)-[:REPRESENTS]->(c2:Country)
    WHERE id(c1) < id(c2)
    WITH id(c1) AS c1Id, id(c2) AS c2Id, count(DISTINCT e) AS weight
    UNWIND [[c1Id, c2Id], [c2Id, c1Id]] AS pair
    RETURN pair[0] AS source,
           pair[1] AS target,
           weight AS weight
  ',
  {validateRelationships: false}
);

CALL gds.louvain.stream('countryMedalCommunities', {relationshipWeightProperty: 'weight'})
YIELD nodeId, communityId
WITH communityId,
     collect(gds.util.asNode(nodeId).name) AS communityCountries
RETURN communityId,
       size(communityCountries) AS member_count,
       communityCountries AS countries
ORDER BY member_count DESC
LIMIT 10;

CALL gds.graph.drop('countryMedalCommunities') YIELD graphName
RETURN graphName;
