//! Build the generated gRPC/protobuf ingress contract.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(
            &["proto/rakka/examples/agent_workflow/v1/agent_workflow.proto"],
            &["proto"],
        )?;
    Ok(())
}
