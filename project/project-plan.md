# Project Plan

## Summary

<!-- Describe your data science project in max. 5 sentences. -->
This project aims to investigate the impact of climate on a power grid that relies more and more on wind/solar energy, as is the case in Germany. 
To this end, the correlation between climate data for Germany and wind/solar energy production is analyzed to help answer research questions such as:

1. Is there a correlation between weather conditions and renewable energy production, and if so, is it linear or exponential?
2. How reliable are renewable energy sources? Do extreme weather conditions affect grid stability?
3. What is the potential for energy storage during peak production periods?
4. What is the trend for renewable energy generation in Germany?

## Rationale

<!-- Outline the impact of the analysis, e.g. which pains it solves. -->
Analyzing the relationship between climate data and renewable energy generation is important for understanding the reliability of renewable energy sources.
The results of this analysis could help readers make data-informed decisions about renewable energy and its use. 
Ideally, this analysis can make a small contribution to the transition to sustainable and efficient energy systems, which is critical to reducing carbon emissions and mitigating climate change.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: DWD Climate Data
* Metadata URL: https://mobilithek.info/offers/-4979349128225020802
* Data URL: https://opendata.dwd.de/climate
* Data Type: various

This dataset contains meteorological measurements and observations.

### Datasource2: Open Power System Data
* Metadata URL: https://data.open-power-system-data.org/time_series/2020-10-06
* Data URL: https://data.open-power-system-data.org/time_series/opsd-time_series-2020-10-06.zip
* Data Type: csv

This dataset contains hourly timeseries data about electricity consumption, wind and solar power generation and capacities, aggregated by country, control area, or bidding zone from 2015-mid 2020.

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Automated data pipeline [#1][i1]
2. Automated tests [#2][i2]
3. Continuous integration [#3][i3]
4. Data exploration & data visualization [#4][i4]
5. Data analysis and question conclusion [#5][i5]
6. Deployment as GitHub page [#6][i6]

[i1]: https://github.com/StealWonders/amse/issues/1
[i2]: https://github.com/StealWonders/amse/issues/2
[i3]: https://github.com/StealWonders/amse/issues/3
[i4]: https://github.com/StealWonders/amse/issues/4
[i5]: https://github.com/StealWonders/amse/issues/5
[i6]: https://github.com/StealWonders/amse/issues/6
