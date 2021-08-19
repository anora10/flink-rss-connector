# Flink RSS Connector

Flink RSS Connector is a Flink connector which facilitates the processing of RSS feeds as data streams.

## Usage

### Example query

```sql
CREATE TEMPORARY TABLE news (
    `title` STRING,
    `description` STRING
) WITH (
    'connector' = 'rss-connector',
    'uri' = 'https://rss.nytimes.com/services/xml/rss/nyt/Europe.xml,'
            'https://rss.nytimes.com/services/xml/rss/nyt/US.xml,'
            'https://rss.nytimes.com/services/xml/rss/nyt/Africa.xml',
    'refresh-interval' = '5000',
    'format' = 'xml'
);

SELECT * FROM news;
```

### Usage remarks
The `rss-connector` connector is always to be used with `xml` format.

Duplicate rows are filtered with a Bloom filter, i.e. the table rows are unique with a high probability.

The `refresh-interval` is an optional attribute that specifies the query interval in ms. It is 10 minutes by default.

The given fields should be named after the required RSS XML tag with a string type. 
Field names not being present will be filled with empty strings.

Multiple URIs may be added to the query, separated by commas.

Some examples of RSS news feeds:
* [Origo](https://www.origo.hu/contentpartner/rss/origoall/origo.xml)
* [Index](https://index.hu/24ora/rss)
* [Spiegel](https://www.spiegel.de/international/index.rss)
* [New York Times Europe](https://rss.nytimes.com/services/xml/rss/nyt/Europe.xml)
* [HVG](https://hvg.hu/rss)