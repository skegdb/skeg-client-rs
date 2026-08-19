// Compile-check of the README's v2 example, with only skeg-client in scope.
use skeg_client::{NativeVectorKindV2, PROTOCOL_V2, SkegClient, VectorBackend, VectorKindV2};

pub async fn readme_example() -> Result<(), Box<dyn std::error::Error>> {
    let mut c = SkegClient::connect_with_version("127.0.0.1:7379", PROTOCOL_V2).await?;
    let caps = c.native_hello().await?;
    if caps.supports(NativeVectorKindV2::Tq2) {
        c.vindex_create_v2("notes", 1024, VectorKindV2::Tq2, VectorBackend::DiskVamana)
            .await?;
    }
    Ok(())
}

fn main() {}
