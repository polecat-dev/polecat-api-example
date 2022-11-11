# Polecat API Example

This script demonstrates how to use the [Polecat API][1] to download documents for a focus company and taxonomy to CSV files. You can refer to it when writing your own client, or run it directly if this is all you need to do.

## Installation
This script is written in [Python](https://python.org). It only uses the Python standard library so installation of additional modules is not required. The only requirement is a Python 3 installation. 

Supported Python version are >= 3.7

Once you have Python 3 installed the only additional setup required is:
1. Clone this repository 
2. Set the `POLECAT_API_TOKEN` environment variable as your API TOKEN

On Linux/Mac the above steps would be the following commands:
1. `git clone https://github.com/polecat-dev/polecat-api-example`
2. `export POLECAT_API_TOKEN=<your-api-token>`

## Usage
The following section assumes you have at least some familiarity with the core concepts of the Polecat API; you have an API token (see https://developer.polecat.com/guides/authentication), you know what a document and insight are (see https://developer.polecat.com/guides/concepts) and you know the parameters of an insight query to use as the script's input (see https://developer.polecat.com/reference/inputs). 

The script will write to multiple CSVs in the present working directory.

The required flags are:
- `--focus`
- `--taxonomy`
- `--to`
- `--from`
These all correspond to the parameters of an insight query https://developer.polecat.com/reference/inputs.

More details about the values expected with those flags as well as details of the flags can be seen using the `--help` flag. 
The other insight query parameters are supported as well as a few special usage flags.

By default the script will fail if running it would overwrite an existing file.
There are two flags to alter this behaviour.
- `--append` will append to existing files
- `--overwrite` will overwrite existing files

Basic example (with made up IDs):
`python3 download_docs.py --focus "dbrldcamrpvr" --taxonomy "f74tj3lb9ebh" --to "2022-11-01" --from "2022-10-01"` 

After looking at the result if you wanted to narrow the results you could add some filters.
E.G. only documents from online media type and in either English or French.
`python3 download_docs.py --focus "dbrldcamrpvr" --taxonomy "f74tj3lb9ebh" --to "2022-11-01" --from "2022-10-01" --language "en" --language "fr" --media "SOCIAL"`

Running the script twice in a row like this would fail as the CSVs would already be present so to refine.
So we need to add the `--overwrite` flag to replace our first result with the second:
`python3 download_docs.py --focus "dbrldcamrpvr" --taxonomy "f74tj3lb9ebh" --to "2022-11-01" --from "2022-10-01" --language "en" --language "fr" --media "SOCIAL" --overwrite`

## Support
For more information about the API itself refer to https://developer.polecat.com
If you have a question about this script in particular please raise a GitHub issue.

## Contributing
If there are changes that you think would be beneficial please feel free to submit a pull request.
Bear in mind that as well as providing some useful functionality this script is also intended as an entry point for new users, showing a simple example of API usage. Any updates shouldn't compromise it's readability or add unnecessary complexity that would make it difficult for a new user to understand.

## License
This project is licensed under the terms of the MIT license.

[1]: https://developer.polecat.com 
