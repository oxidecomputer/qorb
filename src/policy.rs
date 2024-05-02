/// Policy which is applicable to a connection pool.
#[derive(Clone, Debug)]
pub struct Policy {
    /// The desired number of connections that are open and ready for usage.
    pub spare_connections_wanted: usize,

    /// The maximum number of connections which can be opened by this pool.
    pub maximum_connections: usize,

    /// The maximum number of concurrent requests to send to resolvers.
    pub maximum_resolver_concurrency: usize,
}
