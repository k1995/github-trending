# -*- coding: utf-8 -*-
import json
import re
import urllib.parse

import scrapy

from github import settings


endpoint = "https://api.github.com/graphql"

gql = """
{
  search(type: REPOSITORY, first:25, query: "%s") {
    nodes{
      ...on Repository{
        databaseId,
        name,
        description,
        url,
        homepageUrl,
        nameWithOwner,
        languages(first:10){
          edges{
            node{
              name
            }
            size
          }
          totalSize
          totalCount
        },
        primaryLanguage {
          id,
          name
        },
        createdAt,
        updatedAt,
        pushedAt,
        forkCount,
        isArchived,
        stargazers(first: 0){
          totalCount
        }
        repositoryTopics(first: 10){
          nodes{
            id,
            topic{
              name
            }
          }
        }
      }
    }
  }
}
"""


class TrendingSpider(scrapy.Spider):
    """
    Scrape trending data from https://github.com/trending
    """

    name = 'trending'

    def start_requests(self):
        # Filter by data range
        langs = settings.popular_langs + ['chinese']
        for since in ['daily', 'weekly', 'monthly']:
            yield self.build_trending_request(since)
            # Filter by popular language
            for lang in langs:
                yield self.build_trending_request(since, lang)

    @staticmethod
    def build_trending_request(date_range, lang_filter='all'):
        if lang_filter == 'all':
            url = "https://github.com/trending?since={}".format(date_range)
        elif lang_filter == 'chinese':
            url = "https://github.com/trending?since={}&spoken_language_code=zh".format(date_range)
        else:
            lang_encoded = urllib.parse.quote_plus(lang_filter)
            url = "https://github.com/trending/{}?since={}".format(lang_encoded, date_range)
        return scrapy.Request(url, meta={'since': date_range, 'lang_filter': lang_filter})

    def parse(self, response):
        """
        Parse trending page
        """

        search_query = ''
        stars_list = {}
        rank_list = {}
        rank = 1
        for i in response.css(".Box-row"):
            full_name = i.css(".h3 a::attr(href)").extract_first().lstrip('/')
            # New stars count
            stars_inc_str = "".join(i.css(".d-inline-block.float-sm-right::text").extract())
            try:
                stars_inc = int(re.sub("\D", "", stars_inc_str))
            except:
                stars_inc = 0
            if '/' not in full_name:
                continue
            # Cache the new stars count, we'll use it later
            stars_list[full_name] = stars_inc
            rank_list[full_name] = rank
            # Construct graphql api query
            search_query += 'repo:' + full_name + ' '
            rank += 1

        if rank <= 1:
            print("Empty: " + response.url)
            return

        query = {"query": gql % search_query}

        # Request the Graphql API to get detailed information about the repository
        req = scrapy.Request(
            url="https://api.github.com/graphql",
            method='POST',
            callback=self.parse_api_response,
            body=json.dumps(query))
        req.meta['since'] = response.meta['since']
        req.meta['lang_filter'] = response.meta.get('lang_filter')
        req.meta['stars_list'] = stars_list
        req.meta['rank_list'] = rank_list
        req.headers.setdefault('Authorization', "bearer {}".format(settings.token))
        return req

    def parse_api_response(self, response):
        """
        Parse github Graphql API result
        """

        data = json.loads(response.text)
        if 'errors' in data:
            print(data['errors'])
            return
        result = data['data']['search']
        nodes = result['nodes']
        for node in nodes:
            node['since'] = response.meta['since']
            node['lang_filter'] = response.meta['lang_filter']
            node['stars_inc'] = response.meta['stars_list'][node['nameWithOwner']]
            node['rank'] = response.meta['rank_list'][node['nameWithOwner']]
            yield node
