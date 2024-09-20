// <copyright file="UserInfo.cs" company="Microsoft">
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// </copyright>

namespace Icebreaker.Helpers
{
    using System.Collections.Generic;
    using Newtonsoft.Json;

    /// <summary>
    /// Represents a user
    /// </summary>
    public class UserInfo
    {
        /// <summary>
        /// Gets or sets the user's id in Teams (29:xxx).
        /// </summary>
        [JsonIgnore]
        public string UserId { get; set; }

        /// <summary>
        /// Gets or sets the tenant id
        /// </summary>
        [JsonProperty("tenantId")]
        public string TenantId { get; set; }

        /// <summary>
        /// Gets or sets the service URL
        /// </summary>
        [JsonProperty("serviceUrl")]
        public string ServiceUrl { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the user is opted in to pairups
        /// </summary>
        [JsonProperty("optedIn")]
        public bool OptedIn { get; set; }

        /// <summary>
        /// Gets or sets a list of recent pairups
        /// </summary>
        [JsonProperty("recentPairups")]
        public List<UserInfo> RecentPairUps { get; set; }
    }
}