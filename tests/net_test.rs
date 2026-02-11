use bytes::Bytes;
use dsync::dsync::net::{self, Block, Message};
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[test]
fn test_protocol_serialization() -> anyhow::Result<()> {
    let msg = Message::SyncFile {
        path: "/var/lib/postgresql/data/base/1/12345".to_string(),
        size: 1024 * 1024 * 1024,
        mtime: 1_739_276_543,
        checksum: true,
    };

    let bytes = net::serialize_message(&msg);
    let decoded = net::deserialize_message(&bytes);

    if let Message::SyncFile { path, size, .. } = decoded {
        assert_eq!(path, "/var/lib/postgresql/data/base/1/12345");
        assert_eq!(size, 1024 * 1024 * 1024);
    } else {
        anyhow::bail!("Decoded message type mismatch");
    }
    Ok(())
}

#[tokio::test]
async fn test_network_handshake_simulation() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.map_err(|e| anyhow::anyhow!(e))?;
        let mut framed = Framed::new(stream, LengthDelimitedCodec::new());

        // Receive Handshake
        let bytes = framed
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("no message"))?
            .map_err(|e| anyhow::anyhow!(e))?;
        let msg = net::deserialize_message(&bytes);

        if let Message::Handshake { version } = msg {
            assert_eq!(version, "0.1.0");
            // Send back confirmation or next step
            let response = Message::Handshake {
                version: "0.1.0".to_string(),
            };
            let resp_bytes = net::serialize_message(&response);
            framed
                .send(Bytes::from(resp_bytes))
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
        }
        Ok::<(), anyhow::Error>(())
    });

    let client_stream = TcpStream::connect(addr).await?;
    let mut framed = Framed::new(client_stream, LengthDelimitedCodec::new());

    // Send Handshake
    let handshake = Message::Handshake {
        version: "0.1.0".to_string(),
    };
    let bytes = net::serialize_message(&handshake);
    framed.send(Bytes::from(bytes)).await?;

    // Receive Response
    let resp_bytes = framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("no response"))??;
    let resp_msg = net::deserialize_message(&resp_bytes);

    if let Message::Handshake { version } = resp_msg {
        assert_eq!(version, "0.1.0");
    } else {
        anyhow::bail!("Handshake failed");
    }

    server_task.await??;
    Ok(())
}

#[test]
fn test_block_serialization() -> anyhow::Result<()> {
    let block = Block {
        offset: 5000,
        data: vec![1, 2, 3, 4, 5],
    };

    let msg = Message::ApplyBlocks {
        path: "test.bin".to_string(),
        blocks: vec![block],
    };

    let bytes = net::serialize_message(&msg);
    let decoded = net::deserialize_message(&bytes);

    if let Message::ApplyBlocks { blocks, .. } = decoded {
        assert_eq!(blocks.len(), 1);
        let first = blocks.first().ok_or_else(|| anyhow::anyhow!("no blocks"))?;
        assert_eq!(first.offset, 5000);
        assert_eq!(first.data, vec![1, 2, 3, 4, 5]);
    } else {
        anyhow::bail!("Block message mismatch");
    }
    Ok(())
}
