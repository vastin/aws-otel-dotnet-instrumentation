// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using Amazon.Runtime;
using Amazon.Runtime.Credentials;
using Amazon.Runtime.Identity;
using Amazon.Runtime.Internal;
using Amazon.Runtime.Internal.Auth;

/// <summary>
/// Provides AWS authentication and signing capabilities for AWS service requests.
/// </summary>
public interface IAwsAuthenticator
{
    /// <summary>
    /// Asynchronously retrieves AWS credentials that can be used to authenticate requests.
    /// In AWS SDK v4, this returns BaseIdentity which is the base class for all identity types.
    /// </summary>
    /// <returns>
    /// A Task that resolves to a BaseIdentity object containing AWS credentials.
    /// The credentials include access key, secret key, and optional session token.
    /// </returns>
    Task<BaseIdentity> GetCredentialsAsync();

    /// <summary>
    /// Signs an AWS request using AWS Signature Version 4.
    /// </summary>
    void Sign(IRequest request, IClientConfig config, BaseIdentity credentials);
}

/// <summary>
/// Default implementation of IAwsAuthenticator that uses AWS SDK v4's built-in credential
/// and signing mechanisms.
/// </summary>
public class DefaultAwsAuthenticator : IAwsAuthenticator
{
    /// <inheritdoc/>
    public Task<BaseIdentity> GetCredentialsAsync()
    {
        // In AWS SDK v4, use DefaultAWSCredentialsIdentityResolver to get credentials.
        // ResolveIdentity returns AWSCredentials which extends BaseIdentity.
        var resolver = new DefaultAWSCredentialsIdentityResolver();
        return Task.FromResult<BaseIdentity>(resolver.ResolveIdentity(null));
    }

    /// <inheritdoc/>
    public void Sign(IRequest request, IClientConfig config, BaseIdentity credentials)
    {
        // In AWS SDK v4, AWS4Signer.Sign() takes BaseIdentity directly
        new AWS4Signer().Sign(request, config, null, credentials);
    }
}
