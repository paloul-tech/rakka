//! Build generated gRPC/protobuf contracts for the clustered counter example.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(
            &["proto/rakka/examples/clustered_counter/v1/counter.proto"],
            &["proto"],
        )?;
    Ok(())
}
