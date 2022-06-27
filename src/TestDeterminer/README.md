# Test Determiner

Discovers which projects have changed as part of the last commit and then uses the `tests.json` file to determine which test projects should be executed.

This is done to try and keep the GitHub Action as streamlined as possible given it can take several minutes to start a Docker image for SQL Server, for example. We should avoid doing this if there are no SQL Server changes.