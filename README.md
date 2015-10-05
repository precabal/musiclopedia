# musiclopedia
[Musiclopedia](http://musiclopedia.com:5000) is my big data project as part of [Insight Data Engineering](http://insightdataengineering.com/)'s Engineering fellowship program from September 2015 through October 2015.

## What is Musiclopedia?
It is a tool for finding influence between musical artists. It works by looking at references of the artist on web crawl logs published by [CommonCrawl](http://commoncrawl.org/), and building a graph database that interconects two artists when these are mentioned in a number of common websites. Currentlty, it only support Jazz musicians, as the challenge of disambiguating their names is easier since these are typically very unique. 
[IMAGE]

## Motivation
The purpose of the project is to gain experience in processing large amounts of data - the *volume* in the three V's (volume, velocity, variety).
The end product might be interesting for music lovers, and reccomendation engines, since it mines on valuable information that music experts and systems post on the internet.

## Intro
**Meshwork** is an open-source data pipeline which extracts and processes [Common Crawl](http://commoncrawl.org)'s web corpus, finding the [Page Rank](http://ilpubs.stanford.edu:8090/422/1/1999-66.pdf) and 1<sup>st</sup> degree relationships of each web page in the hyperlink graph. To do this, it leverages the following technologies:

- Big Data Pipeline
    - AWS EC2 and S3
    - Apache Hadoop
    - Apache Spark
    - Orient DB
- Front End
    - Flask
    - jQuery
    - D3

## Live Demo
A live demo is currently (July 2015) running at [http://musiclopedia.com:5000](http://musiclopedia.com:5000)

## The Data
The data comes from the [Common Crawl](http://commoncrawl.org)'s July 2015 web corpus. The corpus is ~168TB, but for this project, a subset of this data was processed, roughly 1TB.

The files are available in Common Crawl's public AWS S3 bucket as gzipped WARC file formats. A WARC file contains a header for the whole file, as well as a header and body of each request and web page used and found during the crawl:

![warc-file](github/images/warc-file.png)

The artist list was obtained from the [MusicBrainz database](https://musicbrainz.org/doc/MusicBrainz_Database) by querying the ones with the 'jazz' tag.

## Pipeline Overview
![pipline](github/images/pipeline.png)

The WARC files were processed through a traditional ETL process, but in a distributed manner. The [Spark](https://spark.apache.org) driver was responsible for downloading a gzipped WET file, which was parsed into separate records to be processed by each eorker separately. The process involved comparing a record's text with each entry of the artist list, saving to a [HDFS](http://hadoop.apache.org) CSV files with (url,artist) pairs indicating the matches found. 

A subsequent batch process loaded the CSV files into a distributed [OrientDB graph database](http://orientdb.com/orientdb/), adding vertices for the Artists and Urls, and Edges for the connections between these. From these schema, views were calculated to establish connections between artists based on the statstics of the whole graph. 

The results from these views are stored in the same database as a new type of Edge, which are served using [Flask](http://flask.pocoo.org), [jQuery](https://jquery.com) and [D3](http://d3js.org) to a web server.

## Presentation Deck
Slides are available on [Slideshare](http://www.slideshare.net/PabloRecabal/pablo-recabal-week4demo).

## Languages. Installation