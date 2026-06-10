//! Build generated gRPC/protobuf contracts for the example.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(
            &["proto/rakka/examples/contracts/v1/store.proto"],
            &["proto"],
        )?;
    Ok(())
}
