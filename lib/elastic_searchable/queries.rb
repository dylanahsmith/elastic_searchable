require 'will_paginate/collection'

module ElasticSearchable
  module Queries
    PER_PAGE_DEFAULT = 20

    def search(query, options = {})
      options = options.dup
      options[:index] ||= index_name
      options[:type] ||= index_type

      # determine the number of search results per page
      # supports will_paginate configuration by using:
      # Model.per_page
      # Model.max_per_page
      options[:per_page] ||= self.per_page if self.respond_to?(:per_page)
      if self.respond_to?(:max_per_page)
        per_page = options[:per_page] || ElasticSearchable::Queries::PER_PAGE_DEFAULT
        options[:per_page] = [per_page.to_i, self.max_per_page].min
      end

      ElasticSearchable::Queries.search query, options
    end

    # search returns a will_paginate collection of ActiveRecord objects for the search results
    # supported options:
    # :page - page of results to search for
    # :per_page - number of results per page
    #
    # http://www.elasticsearch.com/docs/elasticsearch/rest_api/search/
    def Queries.search(query, options = {})
      options = options.dup
      page = (options.delete(:page) || 1).to_i
      options[:fields] ||= '_id'
      options[:size] ||= options.delete(:per_page) || ElasticSearchable::Queries::PER_PAGE_DEFAULT
      options[:from] ||= options[:size] * (page - 1)
      if query.is_a?(Hash)
        options[:query] = query
      else
        options[:query] = {
          :query_string => {
            :query => query,
            :default_operator => options.delete(:default_operator)
          }
        }
      end
      query = {}
      case sort = options.delete(:sort)
      when Array,Hash
        options[:sort] = sort
      when String
        query[:sort] = sort
      end

      index_name = options.delete(:index)
      index_type = options.delete(:type)
      index_name ||= "_all" if index_type
      path = ["", index_name, index_type, "_search"].compact.join('/')
      response = ElasticSearchable.request :get, path, :query => query, :json_body => options
      hits = response['hits']

      ranked_results = hits['hits'].collect {|h| [h['_type'], h['_id'].to_i] }
      # Collect all ids of one type to perform one database query per type
      ids_by_type = {}
      ranked_results.each do |index_type, id|
        ids_by_type[index_type] ||= []
        ids_by_type[index_type] <<= id
      end
      objects_by_type = {}
      ids_by_type.each do |index_type, resource_ids|
        objects_by_id = {}
        ElasticSearchable.models[index_type].find(resource_ids).each do |result|
          objects_by_id[result.id] = result
        end
        objects_by_type[index_type] = objects_by_id
      end
      ranked_results.map! { |index_type, id| objects_by_type[index_type][id] }

      page = WillPaginate::Collection.new(page, options[:size], hits['total'])
      page.replace ranked_results
      page
    end
  end
end
