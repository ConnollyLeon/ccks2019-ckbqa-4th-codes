# -*- coding: utf-8 -*-
"""
Created on Tue June 2 16:28:42 2020

@author: peng liang
"""
import json
import time

# from neo4j import GraphDatabase, basic_auth
from GstoreConnector import GstoreConnector

# neo4j
# driver = GraphDatabase.driver("bolt://127.0.0.1:7687", auth=("neo4j", "leon"), encrypted=False)
# session = driver.session()
gc = GstoreConnector('pkubase.gstore.cn', 80, "endpoint", "123")
begintime = time.time()
sparql = "select ?x where { <凯文·杜兰特> <主要奖项> ?x . }"
res = json.loads(gc.query('pkubase', 'json', sparql))
ans = []
for each in res["results"]["bindings"]:
    ans.append(each["x"]["value"])
print(ans)
# session.run('MATCH (n) OPTIONAL MATCH (n)-[r]->() RETURN count(n.name) + count(r)')
# session.run('CREATE INDEX ON:Entity(name)')
endtime = time.time()
print('start neo4j and match all entities,the time is %.2f' % (endtime - begintime))


def GetRelationPaths(entity):
    '''根据实体名，得到所有2跳内的关系路径，用于问题和关系路径的匹配'''
    cql_1 = "select distinct ?x where {{{a} ?x ?y}}".format(a=entity)
    cql_2 = "select distinct ?x ?y where {{{a} ?x ?b. ?b ?y ?z}}".format(a=entity)
    # cql_1 = "match (a:Entity)-[r1:Relation]-() where a.name=$name return DISTINCT r1.name"
    # cql_2 = "match (a:Entity)-[r1:Relation]-()-[r2:Relation]->() where a.name=$name return DISTINCT r1.name,r2.name"
    # res = session.run(cql_1, name=entity)  # 一个多个record组成的集合
    # for record in res:  # 每个record是一个key value的有序序列
    #     rpaths1.append([record['r1.name']])
    # res = session.run(cql_2, name=entity)
    # for record in res:
    #     rpaths2.append([record['r1.name'], record['r2.name']])
    rpaths1 = []
    res = json.loads(gc.query('pkubase', 'json', cql_1))
    for each in res["results"]["bindings"]:
        rpaths1.append([each["x"]["value"]])
    rpaths2 = []
    res = json.loads(gc.query('pkubase', 'json', cql_2))
    for each in res["results"]["bindings"]:
        rpaths2.append([each["x"]["value"], each["y"]["value"]])
    return rpaths1 + rpaths2


def GetRelationPathsSingle(entity):
    '''根据实体名，得到所有1跳关系路径'''
    cql_1 = "select distinct ?x where {{{a} ?x ?y}}".format(a=entity)
    rpaths1 = []
    res = json.loads(gc.query('pkubase', 'json', cql_1))
    for each in res["results"]["bindings"]:
        rpaths1.append([each["x"]["value"]])
    return rpaths1


def GetRelations_2hop(entity):
    '''根据实体名，得到两跳内的所有关系字典，用于问题和实体子图的匹配'''
    # cql = "match (a:Entity)-[r1:Relation]-()-[r2:Relation]->() where a.name=$name return DISTINCT r1.name,r2.name"
    cql_2 = "select distinct ?x ?y where {{{a} ?x ?b. ?b ?y ?z}}".format(a=entity)
    rpaths2 = []
    res = json.loads(gc.query('pkubase', 'json', cql_2))
    try:
        for each in res["results"]["bindings"]:
            rpaths2.append([each["x"]["value"], each["y"]["value"]])
        dic = {}
        for rpath in rpaths2:
            for r in rpath:
                dic[r] = 0
    except:
        dic = {}
    return dic


def GetRelationNum(entity):
    '''根据实体名，得到与之相连的关系数量，代表实体在知识库中的流行度'''
    cql_1 = "select ?x  where {{{a} ?x ?y}}".format(a=entity)
    # cql = "match p=(a:Entity)-[r1:Relation]-() where a.name=$name return count(p)"

    res = json.loads(gc.query('pkubase', 'json', cql_1))
    try:
        return len(res["results"]["bindings"])
    except:
        return 0


def GetTwoEntityTuple(e1, r1, e2):
    cql = "select distinct ?r2 where {{{a} {r1} ?b. ?b ?r2 {e2}.}}".format(a=e1, r1=r1, e2=e2)
    # cql = "match (a:Entity)-[r1:Relation]-(b:Entity)-[r2:Relation]-(c:Entity) where a.name=$e1n and r1.name=$r1n and c.name=$e2n return DISTINCT r2.name"
    tuples = []
    res = json.loads(gc.query('pkubase', 'json', cql))
    # res = session.run(cql, e1n=e1, r1n=r1, e2n=e2)
    for each in res["results"]["bindings"]:
        tuples.append(tuple([e1, r1, each["r2"]["value"], e2]))
    return tuples


def SearchAnsChain(e, r1, r2=None):
    '''对于链式问题，e-r-ans或e-r1-r2-ans，根据最终的实体和关系查询结果'''
    if not r2:
        cql = "select distinct ?x where {{{{{e} {r1} ?x.}} UNION {{?x {r1} {e}.}}}}".format(e=e, r1=r1)
        # cql = "match (a:Entity)-[r1:Relation]-(b) where a.name=$ename and r1.name=$r1name return b.name"

        res = json.loads(gc.query('pkubase', 'json', cql))
        # res = session.run(cql, ename=e, r1name=r1)
        ans = []
        for each in res["results"]["bindings"]:
            ans.append(each["x"]["value"])
    else:
        cql = "select distinct ?x where {{{e} {r1} ?y. ?y {r2} ?x.}}".format(e=e, r1=r1, r2=r2)
        # cql = "match (a:Entity)-[r1:Relation]-()-[r2:Relation]-(b) where a.name=$ename and r1.name=$r1name and r2.name=$r2name return b.name"

        res = json.loads(gc.query('pkubase', 'json', cql))
        ans = []
        for each in res["results"]["bindings"]:
            ans.append(each["x"]["value"])

    return ans


if __name__ == '__main__':
    print(SearchAnsChain('<凯文·杜兰特>', '<主要奖项>'))
    print(SearchAnsChain('<北京>', '<类型>', '<标签>'))
    print(SearchAnsChain('<康佳集团>', '<经营范围>'))
