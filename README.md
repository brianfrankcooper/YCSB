# Project

This repository is a fork of the original YCSB repository. We had to create this fork as there was no response to our pull requests from the YCSB maintainers.

This fork contains the following updates to Cosmos DB binding:
- Cosmos DB Java SDK 4.28.0
- Upgrade to Log4J 2
- Client diagnostic and exception logging to separate files
- Micrometer integration

Additionally, the “operationcount” type in YCSB core, has been changed to long from int to allow for large “operationcount” value.

We will keep this fork up to date with the upstream YCSB repository and push changes from here, back to the upstream YCSB repository if it becomes active again.
## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
