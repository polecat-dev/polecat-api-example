from download_docs import Client
from argparse import ArgumentParser


def search_companies(client, name):
    q = """
        query Companies($first: Int!, $after: Cursor, $search: String) {
            companies(first: $first, after: $after, search: $search) {
                edges { node { id name } }
                pageInfo { endCursor hasNextPage }
            }
        }
    """
    v = {
        "first": client.page_size,
        "search": name
    }
    next_page = True
    companies = {"best": [], "all": []}
    while next_page:
        response = client.execute_query_with_retries(q, v)
        for company in response["data"]["companies"]["edges"]:
            companies["all"] += [(
                company["node"]["name"],
                company["node"]["id"]
            )]
            if company["node"]["name"].lower() == name.lower():
                companies["best"] += [(
                    company["node"]["name"], 
                    company["node"]["id"]
                )]

        v["after"] = response["data"]["companies"]["pageInfo"]["endCursor"]
        next_page = response["data"]["companies"]["pageInfo"]["hasNextPage"]
    
    return companies

def search_taxonomy(client, name):
    q = """
        query Taxonomies{
            myOrganisation{
                taxonomies{
                    id name
                }
            }
        }
    """
    v = {}
    taxonomies = {"best": [], "all": []}
    response = client.execute_query(q, v)
    for taxonomy in response["data"]["myOrganisation"]["taxonomies"]:
        taxonomies["all"] += [(taxonomy["name"], taxonomy["id"])]
        if taxonomy["name"].lower() == name.lower():
            taxonomies["best"] += [(taxonomy["name"], taxonomy["id"])]
    return taxonomies


def output_result(result):
    print("Exact matches: " + str(len(result["best"])))
    if len(result["best"]) > 0:
        output(result["best"])
    print("Total matches: " + str(len(result["all"])))
    if len(result["all"]) > 0:
        output(result["all"])

def output(result):    
    print ("{:<14} {:<70}".format('ID','NAME'))
    for value in result:
        print ("{:<14} {:<70}".format(value[1], value[0]))
    print()


def main():

    parser = ArgumentParser(
        prog="",
        description="",
        epilog="")
    type = parser.add_mutually_exclusive_group(required=True)
    parser.add_argument("--name",
                        help="name to search for",
                        type=str,
                        required=True)
    type.add_argument("--company",
                        help="to search for company",
                        action="store_true")
    type.add_argument("--taxonomy",
                        help="to search for taxonomy",
                        action="store_true")
    args = parser.parse_args()

    client = Client()

    if args.company:
        result = search_companies(client, args.name)
    if args.taxonomy:
        result = search_taxonomy(client, args.name)

    output_result(result)

if __name__ == "__main__":
    main()
