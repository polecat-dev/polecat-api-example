import csv
import json
from os import getenv
from pathlib import Path
from time import sleep
from urllib.error import HTTPError
from urllib.request import urlopen, Request
from argparse import ArgumentParser, RawTextHelpFormatter
from textwrap import dedent

class Client:
    """Client to handle the underlying HTTP calls to the Polecat API.

    A simple class that uses urllib.request to send queries to the 
    Polecat API. Generic configuration that applies to different API
    calls is stored in the class so that only the graphql query and
    variables need to be supplied for query execution.

    Keyword Arguments:
        - token -- API token for authentication. 
            If excluded will check POLECAT_API_TOKEN environment variable.
        - url -- The URL to execute API calls against.
            It is unlikely that the default needs to be overridden. 
            Default ``https://api.polecat.com/graphql``
        - timeout -- The timeout for HTTP calls to the API.
            Default 60
        - page_size -- The number of results to include for paginated queries.
            Default 100
        - max_retry_wait -- The maximum time in seconds to wait between retries.
            If the rate limit is exceeded while executing a request it will
            be retried after waiting for a period of time specified in the
            original response. The request won't be retried if the wait time 
            exceeds this value.
            Default 30
        - max_retries -- The maximum number of successive retries.
            If the rate limit is exceeded while executing a request it will
            be retried. The request won't be retried if it has already been
            tried this many times.
            Default 3

    Methods:
        - execute_query(query, variables) -- Execute a generic Graphql query
            against the Polecat API.
    """

    
    _max_retries_msg = "Maximum number of retries exceeded"

    def __init__(
            self, token="", url="https://api.polecat.com/graphql", 
            timeout=60, page_size=100, max_retry_wait=30, max_retries=3):
        if token == "":
            token = getenv("POLECAT_API_TOKEN", "")
        self.token = token
        self.url = url
        self.timeout = timeout
        self.page_size = page_size
        self.max_retry_wait = max_retry_wait
        self.max_retries = max_retries
        
    def _headers(self):
        """Return headers needed for a Polecat API request"""
        return {
            "Authorization": "api-key " + self.token,
            "Content-Type": "application/json"
        }

    def execute_query(self, query, variables={}):
        """Execute a generic Graphql query against the Polecat API.
        
        Keyword Arguments:
            - query -- A string containing a graphql query.
            - variables -- A dictionary containing variables for the query.
                Default {}
        
        Returns the body of the API response.

        No HTTP exceptions are handled, to handle rate limiting (HTTP Code 429)
        use execute_query_with_retries().
        """
        data = json.dumps(
            {"query": query, "variables": variables}
            ).encode('UTF-8')
        request = Request(url=self.url, data=data, headers=self._headers())
        with urlopen(url=request, timeout=self.timeout) as response:
            body = response.read()
        result = json.loads(body)
        return result

    def execute_query_with_retries(self, query, variables={}):
        """Execute a generic Graphql query against the Polecat API with retries.
        
        Keyword Arguments:
            - query -- A string containing a graphql query.
            - variables -- A dictionary containing variables for the query.
                Default {}
        
        Returns the body of the API response.

        Will handle a 429 HTTP response by retrying the request but will throw
        an exception if the retries exceed either the max_retries configured
        for the class instance.
        No other HTTP exceptions are handled.
        """
        for attempt in range(self.max_retries + 1):
            try:
                return self.execute_query(query, variables)
            except HTTPError as err:
                if err.code != 429:
                    raise
                print("Rate limit exceeded, retrying with backoff") 
                wait = int(err.headers.get("Retry-After"))
                wait = min(wait, self.max_retry_wait)
                if attempt == self.max_retries:
                    raise HTTPError(
                        err.url, err.code, self._max_retries_msg, 
                        err.hdrs, err.fp) from err
                sleep(wait)


def get_documents(client, insight):
    """Get all documents matching an insight.

    Uses the document Graphql query to get all documents matching an insight
    and return those documents in a list.
    
    Keyword Arguments:
        - client -- An instance of the Client class to execute the query.
        - insight -- The insight that documents must match.

    Returns a list of documents.

    Can result in the same exceptions as the execute_query Client method.
    An exception will also be raised if the Graphql response contains an error.
    """
    query = """
        query Documents($insight: InsightQuery!, $first: Int!, $after: Cursor) {
            documents(insight: $insight, first: $first, after: $after, sortAsc: false) {
                edges {
                    node {
                        id harvestTime title domain url 
                        source publisher reach sentiment author
                        companies { company { id name } significance }
                        topics { topic { id name } significance }
                    }
                }
                pageInfo { endCursor hasNextPage }
            }
        }
    """
    variables = {
        "insight": insight,
        "first": client.page_size
    }
    next_page = True
    page_count = 1
    while next_page:
        print("PAGE " + str(page_count) + ":")
        response = client.execute_query_with_retries(query, variables)
        err = response.get("errors")
        if err is not None:
            messages = "\n  ".join([e["message"] for e in err])
            graphql_err = ("Graphql query returned the following error: \n  " 
                          + messages + "\nFull error: " + json.dumps(err))
            raise Exception(graphql_err)
        data = response.get("data") 
        if data is None:
            break
        next_page = data["documents"]["pageInfo"]["hasNextPage"]
        variables["after"] = data["documents"]["pageInfo"]["endCursor"]
        page_count += 1
        yield [edge["node"] for edge in data["documents"]["edges"]]

def write_headers(file, headers):
    with open(file, "w") as fout: 
        writer = csv.writer(fout, dialect="unix", quoting=csv.QUOTE_ALL)
        writer.writerow(headers)

def write_all_headers(existing):
    docs_header = (
        "id",
        "harvest_time",
        "sentiment",
        "reach",
        "publisher",
        "domain",
        "source",
        "author",
        "url",
        "title",
    )

    denorm_header = (
        "document_id",
        "harvest_time",
        "data_source",
        "sentiment",
        "reach",
        "company_id",
        "company_name",
        "company_significance",
        "topic_id",
        "topic_name",
        "topic_significance",
    )

    comp_header = (
        "document_id",
        "company_id",
        "company_name",
        "company_significance",
    )

    tops_header = (
        "document_id",
        "topic_id",
        "topic_name",
        "topic_significance",
    )

    file_headers = {
        "documents.csv": docs_header,
        "documents_denormalised.csv": denorm_header,
        "documents_companies.csv": comp_header,
        "documents_topics.csv": tops_header
    }

    for k, v in file_headers.items():
        if k not in existing:
            write_headers(k, v)

def write_docs(documents, focus_id):
    """Write a list of documents to CSV files.
    
    The CSV structure chosen has been optimised for analysis based on previous
    experience handling the underlying data. The CSVs will be created in the
    directory the script is called from not the directory containing it.

    Keyword Arguments:
        - documents -- A list of documents to include in the CSVs.
        - focus_id -- The id of the company that the documents are linked to.

    Returns nothing.

    Can result in exceptions related to writing files to disk.
    """

    with open("documents.csv", "a") as docs_fout, open(
            "documents_denormalised.csv", "a") as denorm_fout, open(
            "documents_companies.csv", "a") as comp_fout, open(
            "documents_topics.csv", "a") as tops_fout:

        docs_writer = csv.writer(
            docs_fout, dialect="unix", quoting=csv.QUOTE_ALL)

        denorm_writer = csv.writer(
            denorm_fout, dialect="unix", quoting=csv.QUOTE_ALL)

        comp_writer = csv.writer(
            comp_fout, dialect="unix", quoting=csv.QUOTE_ALL)

        tops_writer = csv.writer(
            tops_fout, dialect="unix", quoting=csv.QUOTE_ALL)

        for doc in documents:

            doc_base = [
                doc["id"],
                doc["harvestTime"],
                doc["sentiment"],
                doc["reach"],
            ]

            docs_writer.writerow(
                doc_base
                + [
                    doc["publisher"],
                    doc["domain"],
                    doc["source"],
                    doc["author"],
                    doc["url"],
                    doc["title"],
                ]
            )

            for company in doc["companies"]:
                if company["company"]["id"] == focus_id:
                    for topic in doc["topics"]:
                        denorm_writer.writerow(
                            doc_base
                            + [
                                company["company"]["id"],
                                company["company"]["name"],
                                company["significance"],
                                topic["topic"]["id"],
                                topic["topic"]["name"],
                                topic["significance"],
                            ]
                        )

            for company in doc["companies"]:
                if company["company"]["id"] == focus_id:
                    comp_writer.writerow(
                        (
                            doc["id"],
                            company["company"]["id"],
                            company["company"]["name"],
                            company["significance"],
                        )
                    )

            for topic in doc["topics"]:
                tops_writer.writerow(
                    (
                        doc["id"],
                        topic["topic"]["id"],
                        topic["topic"]["name"],
                        topic["significance"],
                    )
                )


def main():

    parser = ArgumentParser(
        prog="CSV Download",
        description="""Save documents matching an insight query in CSVs.\n
        The arguments are the same as the fields of an insight query.\n
        The arguments focus, taxonomy, from and to are required.\n
        The filter arguments: language, media and sentiment are optional.\n
        To specify multiple filters of a certain type repeat that flag.\n
        See https://developer.polecat.com/reference/enums for valid filter values.
        """,
        epilog="For further info refer to https://developer.polecat.com.",
        formatter_class=RawTextHelpFormatter)
    # To get the help to display nicely the dedent("""\ 
    # pattern is used with a blank line before the closing """
    parser.add_argument("--focus",
                        dest="focus_id",
                        help=dedent("""\
                        The id of the company that the downloaded
                        documents must match. If the ID of the company is
                        unknown then its value can be found using the 
                        Polecat API by executing the companies query.

                        """),
                        type=str,
                        required=True)
    parser.add_argument("--taxonomy",
                        dest="taxonomy_id",
                        help=dedent("""\
                        The id of the taxonomy that the downloaded
                        documents must match. If the ID of the taxonomy is
                        unknown then its value can be found using the 
                        Polecat API by executing the myOrganisation query.

                        """),
                        type=str,
                        required=True)
    parser.add_argument("--from",
                        dest="from_date",
                        help=dedent("""\
                        The earliest date to get documents from. This
                        value is inclusive so documents found on the day given
                        will be included. The date format is yyyy-mm-dd.

                        """),
                        type=str,
                        required=True)
    parser.add_argument("--to",
                        dest="to_date",
                        help=dedent("""\
                        The latest date to get documents from. This
                        value is inclusive so documents found on the day given
                        will be included. The date format is yyyy-mm-dd.

                        """),
                        type=str,
                        required=True)
    parser.add_argument("--language",
                        help=dedent("""\
                        OPTIONAL. The 2 character code for the language
                        of the documents to include. Repeat the flag to include
                        multiple values.

                        """),
                        type=str,
                        action="append",
                        default=[])
    parser.add_argument("--media",
                        help=dedent("""\
                        OPTIONAL. The media type of the documents to
                        include. Repeat the flag to include multiple values.

                        """),
                        type=str,
                        action="append",
                        default=[])
    parser.add_argument("--sentiment",
                        help=dedent("""\
                        OPTIONAL. The sentiment of the documents to
                        include. Repeat the flag to include multiple values.

                        """),
                        type=str,
                        action="append",
                        default=[])
    parser.add_argument("--append",
                        help=dedent("""\
                        OPTIONAL. Append to existing CSVs instead of
                        writing new ones. Headers will not be written and if 
                        there are already CSVs present that would otherwise be 
                        overwritten by this script then the documents will be 
                        appended to those files instead.

                        """),
                        action="store_true")
    parser.add_argument("--overwrite",
                        help=dedent("""\
                        OPTIONAL. Overwrite existing CSVs. Without this
                        flag the script will fail if there are already CSVs
                        present that would be overwritten by this script.

                        """),
                        action="store_true")
    args = parser.parse_args()

    if not args.overwrite:
        existing_files = []
        for csv_type in ["", "_denormalised", "_companies", "_topics"]:
            file = Path("documents" + csv_type + ".csv")
            if file.exists():
                existing_files += [file.name]
        if not args.append and len(existing_files) != 0:  
            raise Exception("Files already exist: " + ", ".join(existing_files)
                            + ". Use --overwrite to overwrite existing files" 
                            + " or use --append to append to existing files.")

    insight = {
        "focusId": args.focus_id,
        "taxonomyId": args.taxonomy_id,
        "fromDate": args.from_date,
        "toDate": args.to_date,
        "languageFilters": [language.lower() for language in args.language],
        "mediaFilters": [media.upper() for media in args.media],
        "sentimentFilters": [sentiment.upper() for sentiment in args.sentiment]
    }

    client = Client()

    docs = get_documents(client, insight)

    total_docs = 0
    append = args.append
    if append:
        write_all_headers(existing_files)
    else:
        write_all_headers([])
    for page in docs:
        print("WRITING...")
        write_docs(page, args.focus_id)
        print("...WRITTEN")
        total_docs += len(page)
        append = True

    print("FINISHED")
    print("Total of " + str(total_docs) + " documents matched.")

if __name__ == "__main__":
    main()
