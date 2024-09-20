// <copyright file="IcebreakerBotDataProvider.cs" company="Microsoft">
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// </copyright>

namespace Icebreaker.Helpers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Icebreaker.Interfaces;
    using Microsoft.ApplicationInsights;
    using Microsoft.ApplicationInsights.DataContracts;
    using Microsoft.Azure;
    using Microsoft.Azure.Cosmos;
    using Microsoft.Azure.Cosmos.Linq;
    using Microsoft.Bot.Schema.Teams;

    /// <summary>
    /// Data provider routines
    /// </summary>
    public class IcebreakerBotDataProvider : IBotDataProvider
    {
        // Request the minimum throughput by default
        private const int DefaultRequestThroughput = 400;

        private readonly TelemetryClient telemetryClient;
        private readonly Lazy<Task> initializeTask;
        private readonly ISecretsHelper secretsHelper;
        private CosmosClient cosmosClient;
        private Database database;
        private Container teamsCollection;
        private Container usersCollection;

        /// <summary>
        /// Initializes a new instance of the <see cref="IcebreakerBotDataProvider"/> class.
        /// </summary>
        /// <param name="telemetryClient">The telemetry client to use</param>
        /// <param name="secretsHelper">Secrets helper to fetch secrets</param>
        public IcebreakerBotDataProvider(TelemetryClient telemetryClient, ISecretsHelper secretsHelper)
        {
            this.telemetryClient = telemetryClient;
            this.secretsHelper = secretsHelper;
            this.initializeTask = new Lazy<Task>(() => this.InitializeAsync());
        }

        /// <summary>
        /// Updates team installation status in store. If the bot is installed, the info is saved, otherwise info for the team is deleted.
        /// </summary>
        /// <param name="team">The team installation info</param>
        /// <param name="installed">Value that indicates if bot is installed</param>
        /// <returns>Tracking task</returns>
        public async Task UpdateTeamInstallStatusAsync(TeamInstallInfo team, bool installed)
        {
            await this.EnsureInitializedAsync();

            try
            {
                if (installed)
                {
                    await this.teamsCollection.UpsertItemAsync(team, new PartitionKey(team.TeamId));
                }
                else
                {
                    await this.teamsCollection.DeleteItemAsync<TeamInstallInfo>(team.TeamId, new PartitionKey(team.TeamId));
                }
            }
            catch (CosmosException ex)
            {
                this.telemetryClient.TrackException(ex);
                // Optionally handle specific status codes, such as NotFound for delete operations
            }
        }

        /// <summary>
        /// Get the list of teams to which the app was installed.
        /// </summary>
        /// <returns>List of installed teams</returns>
        public async Task<IList<TeamInstallInfo>> GetInstalledTeamsAsync()
        {
            await this.EnsureInitializedAsync();

            var installedTeams = new List<TeamInstallInfo>();

            try
            {
                var query = this.teamsCollection.GetItemLinqQueryable<TeamInstallInfo>(requestOptions: new QueryRequestOptions
                {
                    MaxItemCount = -1,
                    MaxConcurrency = -1,
                }).ToFeedIterator();

                while (query.HasMoreResults)
                {
                    var response = await query.ReadNextAsync();
                    installedTeams.AddRange(response);
                }
            }
            catch (CosmosException ex)
            {
                this.telemetryClient.TrackException(ex);
            }

            //// approach 2
            //var installedTeams2 = new List<TeamInstallInfo>();

            //using (FeedIterator<TeamInstallInfo> resultSetIterator = this.teamsCollection.GetItemQueryIterator<
            //    TeamInstallInfo>(requestOptions: new QueryRequestOptions
            //    {
            //        MaxItemCount = -1,
            //        MaxConcurrency = -1,
            //    }))
            //{
            //    while (resultSetIterator.HasMoreResults)
            //    {
            //        var response = await resultSetIterator.ReadNextAsync();
            //        installedTeams2.AddRange(response);
            //    }
            //}

            return installedTeams;
        }

        /// <summary>
        /// Returns the team that the bot has been installed to
        /// </summary>
        /// <param name="teamId">The team id</param>
        /// <returns>Team that the bot is installed to</returns>
        public async Task<TeamInstallInfo> GetInstalledTeamAsync(string teamId)
        {
            await this.EnsureInitializedAsync();

            try
            {
                ItemResponse<TeamInstallInfo> response = await this.teamsCollection.ReadItemAsync<TeamInstallInfo>(
                    teamId, new PartitionKey(teamId));

                return response.Resource;
            }
            catch (CosmosException ex)
            {
                this.telemetryClient.TrackException(ex);
                return null;
            }
        }

        /// <summary>
        /// Get the stored information about the given user
        /// </summary>
        /// <param name="userId">User id</param>
        /// <returns>User information</returns>
        public async Task<UserInfo> GetUserInfoAsync(string userId)
        {
            await this.EnsureInitializedAsync();

            try
            {
                ItemResponse<UserInfo> response = await this.usersCollection.ReadItemAsync<UserInfo>(
                    userId, new PartitionKey(userId));

                return response.Resource;
            }
            catch (CosmosException ex)
            {
                this.telemetryClient.TrackException(ex);
                return null;
            }
        }

        /// <summary>
        /// Get the stored information about given users
        /// </summary>
        /// <returns>User information</returns>
        public async Task<Dictionary<string, bool>> GetAllUsersOptInStatusAsync()
        {
            await this.EnsureInitializedAsync();

            var usersOptInStatusLookup = new Dictionary<string, bool>();
            try
            {
                var query = this.usersCollection.GetItemLinqQueryable<UserInfo>(
                    requestOptions: new QueryRequestOptions
                    {
                        MaxItemCount = -1,
                        MaxConcurrency = -1,
                    })
                    .Select(u => new UserInfo { UserId = u.UserId, OptedIn = u.OptedIn })
                    .ToFeedIterator();

                while (query.HasMoreResults)
                {
                    var responseBatch = await query.ReadNextAsync();
                    foreach (var userInfo in responseBatch)
                    {
                        usersOptInStatusLookup.Add(userInfo.UserId, userInfo.OptedIn);
                    }
                }

                return usersOptInStatusLookup;
            }
            catch (Exception ex)
            {
                this.telemetryClient.TrackException(ex.InnerException);
                return null;
            }
        }

        /// <summary>
        /// Set the user info for the given user
        /// </summary>
        /// <param name="tenantId">Tenant id</param>
        /// <param name="userId">User id</param>
        /// <param name="optedIn">User opt-in status</param>
        /// <param name="serviceUrl">User service URL</param>
        /// <returns>Tracking task</returns>
        public async Task SetUserInfoAsync(string tenantId, string userId, bool optedIn, string serviceUrl)
        {
            await this.EnsureInitializedAsync();

            var userInfo = new UserInfo
            {
                TenantId = tenantId,
                UserId = userId,
                OptedIn = optedIn,
                ServiceUrl = serviceUrl,
            };
            await this.usersCollection.UpsertItemAsync(userInfo);
        }

        /// <summary>
        /// Initializes the database connection.
        /// </summary>
        /// <returns>Tracking task</returns>
        private async Task InitializeAsync()
        {
            this.telemetryClient.TrackTrace("Initializing data store");

            var endpointUrl = CloudConfigurationManager.GetSetting("CosmosDBEndpointUrl");
            var databaseName = CloudConfigurationManager.GetSetting("CosmosDBDatabaseName");
            var teamsCollectionName = CloudConfigurationManager.GetSetting("CosmosCollectionTeams");
            var usersCollectionName = CloudConfigurationManager.GetSetting("CosmosCollectionUsers");

            this.cosmosClient = new CosmosClient(endpointUrl, this.secretsHelper.CosmosDBKey);

            bool useSharedOffer = true;

            // Create the database if needed
            try
            {
                this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(databaseName, DefaultRequestThroughput);
            }
            catch (CosmosException ex)
            {
                if (ex.Message?.Contains("SharedOffer is Disabled") ?? false)
                {
                    this.telemetryClient.TrackTrace("Database shared offer is disabled for the account, will provision throughput at container level", SeverityLevel.Information);
                    useSharedOffer = false;

                    this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(databaseName);
                }
                else
                {
                    throw;
                }
            }

            // Get a reference to the Teams collection, creating it if needed
            var teamsCollectionDefinition = new ContainerProperties
            {
                Id = teamsCollectionName,
                PartitionKeyPath = "/id",
            };
            this.teamsCollection = await this.cosmosClient.GetDatabase(databaseName).CreateContainerIfNotExistsAsync(teamsCollectionDefinition, useSharedOffer ? -1 : DefaultRequestThroughput);

            // Get a reference to the Users collection, creating it if needed
            var usersCollectionDefinition = new ContainerProperties
            {
                Id = usersCollectionName,
                PartitionKeyPath = "/id",
            };
            this.usersCollection = await this.cosmosClient.GetDatabase(databaseName).CreateContainerIfNotExistsAsync(usersCollectionDefinition, useSharedOffer ? -1 : DefaultRequestThroughput);

            this.telemetryClient.TrackTrace("Data store initialized");
        }

        private async Task EnsureInitializedAsync()
        {
            await this.initializeTask.Value;
        }
    }
}