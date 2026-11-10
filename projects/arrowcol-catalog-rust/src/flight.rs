use std::pin::Pin;

use arrow_flight::{
    Action, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, PollInfo, PutResult,
    SchemaResult, Ticket,
    flight_service_server::FlightService,
    sql::{Any, Command, DoPutUpdateResult},
};
use arrowcol_flight::encode_ddl_completion_response;
use async_trait::async_trait;
use futures::StreamExt;
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

use crate::{CatalogService, DdlIdentityPolicy, identity::now_unix_millis};

/// Flight SQL transport retaining the committed Arrowcol DDL completion headers.
#[derive(Clone)]
pub struct CatalogFlightService {
    catalog: CatalogService,
    ddl_identity_policy: DdlIdentityPolicy,
}

impl CatalogService {
    /// Creates the Flight endpoint, preserving standard SQL queries and DDL metadata.
    ///
    /// DDL is rejected unless a caller explicitly installs an authentication policy.
    #[must_use]
    pub fn flight_service(&self) -> CatalogFlightService {
        self.flight_service_with_ddl_identity(DdlIdentityPolicy::reject())
    }

    /// Creates the Flight endpoint with an explicit catalog-DDL identity policy.
    #[must_use]
    pub fn flight_service_with_ddl_identity(&self, ddl_identity_policy: DdlIdentityPolicy) -> CatalogFlightService {
        CatalogFlightService {
            catalog: self.clone(),
            ddl_identity_policy,
        }
    }
}

#[async_trait]
impl FlightService for CatalogFlightService {
    type HandshakeStream = <CatalogService as FlightService>::HandshakeStream;
    type ListFlightsStream = <CatalogService as FlightService>::ListFlightsStream;
    type DoGetStream = <CatalogService as FlightService>::DoGetStream;
    type DoPutStream = <CatalogService as FlightService>::DoPutStream;
    type DoExchangeStream = <CatalogService as FlightService>::DoExchangeStream;
    type DoActionStream = <CatalogService as FlightService>::DoActionStream;
    type ListActionsStream = <CatalogService as FlightService>::ListActionsStream;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        self.catalog.handshake(request).await
    }

    async fn list_flights(&self, request: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {
        self.catalog.list_flights(request).await
    }

    async fn get_flight_info(&self, request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        self.catalog.get_flight_info(request).await
    }

    async fn poll_flight_info(&self, request: Request<FlightDescriptor>) -> Result<Response<PollInfo>, Status> {
        self.catalog.poll_flight_info(request).await
    }

    async fn get_schema(&self, request: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        self.catalog.get_schema(request).await
    }

    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        self.catalog.do_get(request).await
    }

    async fn do_put(&self, request: Request<Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        self.ddl_identity_policy.verify(request.metadata(), now_unix_millis())?;
        let mut request = request.map(StreamExt::peekable);
        let first = tokio::time::timeout(std::time::Duration::from_secs(10), Pin::new(request.get_mut()).peek())
            .await
            .map_err(|_| Status::deadline_exceeded("catalog DDL descriptor did not arrive"))?
            .cloned()
            .ok_or_else(|| Status::invalid_argument("catalog DDL descriptor is missing"))??;
        let descriptor = first
            .flight_descriptor
            .ok_or_else(|| Status::invalid_argument("catalog DDL descriptor is missing"))?;
        if descriptor.r#type != arrow_flight::flight_descriptor::DescriptorType::Cmd as i32 {
            return Err(Status::invalid_argument("catalog DDL requires a command descriptor"));
        }
        let any = Any::decode(descriptor.cmd)
            .map_err(|error| Status::invalid_argument(format!("invalid catalog DDL command: {error}")))?;
        let command = Command::try_from(any)
            .map_err(|error| Status::invalid_argument(format!("invalid catalog DDL command: {error}")))?;
        let Command::CommandStatementUpdate(command) = command else {
            return Err(Status::unimplemented("catalog DoPut supports statement updates only"));
        };
        let completion = self.catalog.apply_statement_update(command, request).await?;
        let (result, metadata) = match completion {
            Some(completion) => {
                let encoded = encode_ddl_completion_response(&completion).map_err(|error| error.into_status())?;
                let (result, metadata, _) = encoded.into_parts();
                (result, metadata)
            }
            None => (
                PutResult {
                    app_metadata: DoPutUpdateResult { record_count: 0 }.encode_to_vec().into(),
                },
                tonic::metadata::MetadataMap::new(),
            ),
        };
        let mut response: Response<Self::DoPutStream> =
            Response::new(Box::pin(futures::stream::once(async move { Ok(result) })));
        *response.metadata_mut() = metadata;
        Ok(response)
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        self.catalog.do_exchange(request).await
    }

    async fn do_action(&self, request: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        self.catalog.do_action(request).await
    }

    async fn list_actions(&self, request: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        self.catalog.list_actions(request).await
    }
}
