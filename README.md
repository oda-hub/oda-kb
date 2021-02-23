# ODA Knowledge Base

_browse, retrieve, compute on living collection of data, services, and codes_

## What is *living* in this case?

*living* means that the KB contains not only data (static and dynamic) but also references to executable workflows, on equal basis with the data. Computing of these workflows is generally used to populate the KB itself, by creaging new data or workflows.

## What is this KB anyway?

it's just an [RDF](https://www.w3.org/RDF/) graph, accessible for example through a [SPARQL](https://www.w3.org/2001/sw/wiki/SPARQL) [endpoint](https://sparql.odahub.io/).

# ontology?

[Ontologies](https://www.w3.org/standards/semanticweb/ontology) are important to classify, validate, and organize the KB.  Ontologies are commonly defined before the data is ingested, but can be (perhaps) also derived from the evolving data archive.

# use cases - in production

## literature parsing

Astronomy-related arXiv, GCNs, and ATels are parsed in real-time and the data is stored in the KB.

A [view](https://in.odahub.io/odatests/papers) of these data is available although restricted.

These data are futher used in designing scientifically useful workflows.

## devising and performing experiments on the ODA platform

The KB describes, in particular, what kind of workflow (in this case, queries) can be made on the ODA online analysis platform. The descritions is sufficient to design and execute the computational experiments.
This capacity is used in several use cases.

### self-testing online analysis

[Expertiment Results](https://in.odahub.io/odatests/data) are produced by an [application](https://github.com/volodymyrss/oda-experiments-deployment) based on the avaiable information about the platform capabilities. These results thereform prove the platform features.

### reaction to elusive scientific transients

Studies of elusive short and energetic multi-messenger transients has become (in the last years) a core activity of many astrophysical observatories, including INTEGRAL.

Combining parsed literature, dedicated events streams, and known scientific workflows (e.g. those provided by the ODA platform) the KB creates the basis for the realtime reation to new scientific events.

### instrument cross-calibration workflows

Following a [proposal](https://zenodo.org/record/3559528), the KB organizes cross-calibration workflows, comparing the observation of the same astrophysical sources from different scientific instruments, observatories, telescopes, and detectors.

### evolving context for the INTEGRAL data center operations 

Quick look analysis for INTEGRAL first-level sciencific elaboration uses scientific context from ODA kb (in part, derived from the real-time literature).

### How do I set the endpoint?

export ODA_SPARQL_ROOT=http://fuseki.internal.odahub.io/dataanalysis

# *[prototype]*

## example discovery and execution of a workflow

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


h1. Knowledge Base


h2. What is this, who needs it, another complex technology?

In order to systematically share complex data in structured but flexibly manner, W3C created technology to describe relations, the RDF.

Astronomers adopted it:

http://ivoa.net/rdf/

Various massive public data resources use it:

http://dbpedia.org/page/Crab_Pulsar

The world (google) is now convinced:

https://schema.org/

And everybody shares structured data:

https://datasetsearch.research.google.com/search?query=spi-acs

Function ontology allows to define entities which are commutable, given certain input parameters.

http://fno.io/

Think of this diverse collection of RDF vocabularies and ontologies as *language of structured knowledge in the Web*. Of pretty much any kind.


h2. But really, why will this be useful for anyone?

Semantic Web initiatives started to regularly emerge decades ago, and usually failed after some deliberation. Since google has now adopted it, it's going to win.

h2. Sounds convincing! What did we already do about it?

We deployed a database, and extra tooling to access it:

https://sparql.odahub.io/

internal-only access http://fuseki.internal.odahub.io/

https://github.com/volodymyrss/oda-kb

h2. How do I set the endpoint?

export ODA_SPARQL_ROOT=http://fuseki.internal.odahub.io/dataanalysis

h2. And real-life applications?

INTEGRAL QLA uses the KB to learn and share what's interesting in the data.

ODA platform integration self-testing uses KB to automatically design and execute possible tests:

https://in.odahub.io/odatests/data

Turns out, this testing is very similar to automatically designing and executing many "scientific" workflows:

https://in.odahub.io/odatests/papers

List of Relevant APIs can be also discovered.




