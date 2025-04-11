from neo4j import GraphDatabase

class Interface:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self._driver.verify_connectivity()

    def close(self):
        self._driver.close()

    def bfs(self, start_node, last_node):
        # TODO: Implement this method        
        session=self._driver.session()
        result=session.run("CALL gds.graph.exists('bfs_graph')")
        if result.data()[0]['exists']==False:
            create_graph="""CALL gds.graph.project.cypher(
                                     'bfs_graph',
                                     'MATCH (n) RETURN id(n) AS id',
                                     'MATCH (n)-[r:Trip]->(m) RETURN id(n) AS source, id(m) AS target, r.distance AS distance, r.fare AS fare, r.pickup_dt as pick, r.dropoff_dt AS drop'
                                     )"""        
            session.run(create_graph)
        result=session.run("MATCH (a:Location{name:"+str(start_node)+"}),(b:Location{name:"+str(last_node)+"}) \
                           WITH id(a) AS source,id(b) AS target \
                           CALL gds.bfs.stream('bfs_graph', \
                                               {sourceNode:source,targetNodes:target}) \
                           YIELD path RETURN path")
        #print(result.data())
        return result.data()
        #raise NotImplementedError

    def pagerank(self, max_iterations, weight_property):
        # TODO: Implement this method        
        session=self._driver.session()
        result=session.run("CALL gds.graph.exists('pg_graph')")
        if result.data()[0]['exists']==False:
            create_graph="""CALL gds.graph.project.cypher(
                                     'pg_graph',
                                     'MATCH (n) RETURN id(n) AS id',
                                     'MATCH (n)-[r:Trip]->(m) RETURN id(n) AS source, id(m) AS target, r.distance AS distance, r.fare AS fare, r.pickup_dt as pick, r.dropoff_dt AS drop'
                                     )"""        
            session.run(create_graph)
        #session.run("CALL gds.pageRank.write('my_graph', \
         #           {maxIterations:"+str(max_iterations)+", \
          #          writeProperty: 'pagerank', \
           #         weightProperty: '"+weight_property+"'})")
        result=session.run("CALL gds.pageRank.stream('pg_graph', \
                            {maxIterations:"+str(max_iterations)+", \
                            relationshipWeightProperty: '"+weight_property+"'}) \
                            YIELD nodeId, score \
                            RETURN gds.util.asNode(nodeId).name AS name,score \
                            ORDER BY score DESC")
        
        data=result.data()
        return [data[0],data[-1]]
        #raise NotImplementedError

