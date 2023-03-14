# runneR

## What is runneR?
This repo was created to scrape and aggregate NCAA track & cross country results. Unlike sports like the NFL or NBA, distance running doesn't have a de facto source for statistics like ESPN or Pro Football Reference. As a former collegiate runner and endurance afficionado, this was an attempt to bring some of that data to the masses. 

## How are the files broken up?
Most of the files are found between the `scraper` and `scraper_v2` folders for loading the data. I'm still in the process of building out a full database for others to use, mostly due to the cost of hosting in a cloud environment. It used to be in an AWS RDS Postgres database but the uptime was more money than I was interested in paying for a personal project. I'm currently looking into different options that would allow me to host this for minimal cost and then generate an R library for others to easily use the data.

## Tell me about the Shiny app
When the AWS instance was up, I was utilizing the code in the `TrackPowerRankings/runneR/runneR` folder to run a Shiny app which allowed users to see power rankings for NCAA runners. Rather than doing the "sort by time" approach, I compiled scores for runners across multiple events to allow individuals to compare runners across events, genders, and divisions. It even included historical athletes to see how someone from 2015 compared to someone in 2022, accouting for time "inflation" with the introduction of things like super-spikes.

The app also provided a race simulation feature, where the user could pick a specific event and subset of athletes to see how the race may unfold. This approach used a Monte Carlo setup based on historic performances to identify how results might play out.

## Why is it no longer hosted?
Hosting the database on AWS was expensive for a personal project. As noted above, I'm actively looking into cheaper ways to host in order to make it accessible for others.
