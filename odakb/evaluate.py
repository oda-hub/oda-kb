import os
import glob
import yaml

import nb2workflow.nbadapter as nba

def build_local_context():
    context = {}

    # also deduce from gitlhub reference
    for oda_yaml in glob.glob("code/*/oda.yaml"):
        y = yaml.load(open(oda_yaml))
        print("loading oda yaml", oda_yaml, y)
        context[y['uri_base']] = os.path.dirname(oda_yaml)

    return context


def evaluate(query):
    context = build_local_context()

    if query.startswith("http://"):
        if query in context:
            query_alias = query.replace("http://","").replace("/",".") 
        
            print("will evaluate with local context", context[query])

            fn = "data/{}.yaml".format(query_alias)

            if os.path.exists(fn):
                d=yaml.load(open(fn))

            else:
                nbnames = glob.glob(context[query])
                assert len(nbnames) == 1
                nbname = nbnames[0]
            
                d = nba.nbrun(nbname, {})

                for k, v in d.items():
                    try:
                        d[k] = yaml.load(v)
                    except: pass

                try:
                    os.makedirs("data")
                except:
                    pass
            
                yaml.dump(d, open(fn, "w"))

            return d

            

    
    
