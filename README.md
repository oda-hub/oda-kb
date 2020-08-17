# ODA Knowledge Base

_browse, retrieve, compute of data, services, and codes_

# example *[prototype]*

```bash
$ oda find --input oda:integral-scw --output oda:sky-image
oda-ddosa:ii_skyimage
    oda:input_scw oda:integral-scw;
    oda:input_emin ivoa:emin;
    oda:input_emax ivoa:emax;
    oda:callType oda:ddamodule .

oda-service:ii_skyimage
    oda:input_scw oda:integral-scw;
    oda:input_emin ivoa:emin;
    oda:input_emax ivoa:emax;
    oda:callType oda:odaapi .

$ oda run 

```

# federate

```bash
$ oda knowledge -v
local file://..
http http://..
sparql http://..
```

# find

start from root
make all thinks referring to root

# run

execution method:

* active execution
* active request
* worker
* callback

# reasonong

disconneceted realms
reason derivations 

reasoning is based on rules, e.g. transitive:

?x a ?y . ?y a ?z => ?x a ?z

or

?x oda:blocks ?y => ?y oda:blocked-by ?x

rule might generate another rule

emergent geometry


# ontology?

[Ontologies](https://www.w3.org/standards/semanticweb/ontology) are important to classify, validate, and organize the KB.  Ontologies are commonly defined before the data is ingested, but can be (perhaps) also derived from the evolving data archive.
