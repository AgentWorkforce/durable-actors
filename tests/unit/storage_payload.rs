use crate::payload::{
    IO_BUFFER_BYTES, JsonBody, JsonPayload, JsonStream, JsonText, Payload, Piece, Spool,
};
use anyhow::Result;
use serde_json::{Value, json};
use std::io::Write;

#[test]
fn json_text_keeps_the_buffer_it_was_read_into() -> Result<()> {
    let bytes = br#"{"rows":[1,2,3]}"#.to_vec();
    let address = bytes.as_ptr();
    let text = JsonText::from_vec(bytes)?;
    assert_eq!(text.get().as_ptr(), address);
    assert_eq!(text.clone().bytes().as_ptr(), address);
    assert!(JsonText::from_vec(br#"{"rows":"#.to_vec()).is_err());
    assert!(JsonText::from_vec(b"1 2".to_vec()).is_err());
    Ok(())
}

#[tokio::test]
async fn json_bodies_send_a_field_from_its_own_text() -> Result<()> {
    use http_body::Body as _;
    let rows = JsonText::from(json!([1, 2]));
    for (object, expected) in [
        (json!({}), json!({"rows": [1, 2]})),
        (
            json!({"type": "completed"}),
            json!({"type": "completed", "rows": [1, 2]}),
        ),
    ] {
        let body = JsonBody::with_field(&object, "rows", rows.clone().into())?;
        assert!(matches!(&body.0[1], Piece::Ready(bytes) if bytes.as_ptr() == rows.get().as_ptr()));
        let length = body.size_hint().exact();
        let bytes = axum::body::to_bytes(axum::body::Body::new(body), usize::MAX).await?;
        assert_eq!(length, Some(bytes.len() as u64));
        assert_eq!(serde_json::from_slice::<Value>(&bytes)?, expected);
    }
    assert_eq!(
        JsonBody::with_field(&json!({}), "rows", rows.clone().into())?
            .into_text()
            .await?
            .get(),
        r#"{"rows":[1,2]}"#
    );
    assert!(JsonBody::with_field(&json!([]), "rows", rows.into()).is_err());
    Ok(())
}

#[tokio::test]
async fn json_bodies_send_a_field_as_it_arrives_with_its_announced_length() -> Result<()> {
    use http_body::Body as _;
    let (chunks, _open) = sent(&["[1,", "2]"]);
    let mut stream = JsonStream::new(chunks, 5);
    let finished = stream.finished();
    let body = JsonBody::with_field(&json!({"type": "completed"}), "rows", stream.into())?;
    let expected = r#"{"type":"completed","rows":[1,2]}"#;
    assert_eq!(body.size_hint().exact(), Some(expected.len() as u64));
    let bytes = axum::body::to_bytes(axum::body::Body::new(body), usize::MAX).await?;
    assert_eq!(bytes.as_ref(), expected.as_bytes());
    assert!(finished.await.is_err());
    Ok(())
}

#[tokio::test]
async fn an_arriving_document_that_ends_short_or_runs_long_fails() -> Result<()> {
    for (part, problem) in [
        ("[1,", "ended before all of it arrived"),
        ("[1,2,3]", "arrived longer than announced"),
    ] {
        let (chunks, open) = sent(&[part]);
        drop(open);
        let error = JsonPayload::from(JsonStream::new(chunks, 5))
            .into_text()
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains(problem), "{error:#}");
        let (chunks, open) = sent(&[part]);
        drop(open);
        let body = JsonBody::with_field(&json!({}), "rows", JsonStream::new(chunks, 5).into())?;
        assert!(
            axum::body::to_bytes(axum::body::Body::new(body), usize::MAX)
                .await
                .is_err()
        );
    }
    Ok(())
}

/// A stream of `parts`, which stays open until the returned sender is dropped.
fn sent(
    parts: &[&str],
) -> (
    impl futures_util::Stream<Item = Result<bytes::Bytes>> + Send + 'static,
    tokio::sync::mpsc::Sender<bytes::Bytes>,
) {
    use futures_util::StreamExt;
    let (sender, receiver) = tokio::sync::mpsc::channel(parts.len());
    for part in parts {
        sender
            .try_send(bytes::Bytes::copy_from_slice(part.as_bytes()))
            .expect("room for every part");
    }
    (
        tokio_stream::wrappers::ReceiverStream::new(receiver).map(Ok),
        sender,
    )
}

#[test]
fn large_payloads_spill_to_a_file_their_slices_keep() -> Result<()> {
    let mut spool = Spool::new();
    for n in 0..256u32 {
        spool.write_all(&n.to_le_bytes().repeat(1024))?;
    }
    let payload = spool.finish()?;
    assert!(matches!(payload, Payload::File(_)));
    let tail = payload.slice(255 * 4096..);
    drop(payload);
    assert_eq!(tail.to_bytes()?.as_ref(), 255u32.to_le_bytes().repeat(1024));
    assert_eq!(tail.read(4..8)?.as_ref(), 255u32.to_le_bytes());
    let mut small = Spool::new();
    small.write_all(b"small")?;
    assert!(matches!(small.finish()?, Payload::Memory(_)));
    Ok(())
}

#[tokio::test]
async fn file_payloads_read_and_compare_like_their_bytes() -> Result<()> {
    use futures_util::TryStreamExt;
    use std::io::Read;
    let expected: Vec<u8> = (0..2 * IO_BUFFER_BYTES + 17).map(|n| n as u8).collect();
    let file = crate::payload::copy(expected.as_slice())?;
    assert!(matches!(file, Payload::File(_)));
    let memory = Payload::from(expected.clone());
    assert_eq!(file, memory);
    assert_ne!(file.slice(1..), memory.slice(..memory.len() - 1));
    assert!(file.starts_with(&expected[..4])?);
    let mut read = Vec::new();
    file.slice(3..).reader().read_to_end(&mut read)?;
    assert_eq!(read, expected[3..]);
    let mut visited = Vec::new();
    file.for_each_chunk(|chunk| {
        assert!(chunk.len() <= IO_BUFFER_BYTES);
        visited.extend_from_slice(chunk);
        Ok(())
    })?;
    assert_eq!(visited, expected);
    let chunks: Vec<bytes::Bytes> = file.chunks().try_collect().await?;
    assert!(chunks.iter().all(|chunk| chunk.len() <= IO_BUFFER_BYTES));
    assert_eq!(chunks.concat(), expected);
    Ok(())
}

#[tokio::test]
async fn uploads_have_bounded_chunks_and_replay_the_requested_offset() -> Result<()> {
    use google_cloud_storage::streaming_source::{Seek, StreamingSource};
    let bytes = vec![17; 1024 * 1024 + 7];
    for payload in [
        Payload::from(bytes.clone()),
        crate::payload::copy(bytes.as_slice())?,
    ] {
        let mut upload = crate::payload::Upload::new(payload);
        let mut received = Vec::new();
        while let Some(chunk) = upload.next().await {
            let chunk = chunk?;
            assert!(chunk.len() <= 256 * 1024);
            received.extend_from_slice(&chunk);
        }
        assert_eq!(received, bytes);
        upload.seek(1024 * 1024).await?;
        assert_eq!(upload.next().await.unwrap()?.as_ref(), &[17; 7]);
        assert!(upload.next().await.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn downloads_bound_pending_bytes_and_preserve_the_final_partial_batch() -> Result<()> {
    use crate::payload::Download;
    use bytes::Bytes;

    let expected = Bytes::from((0..1024 * 1024 + 17).map(|n| n as u8).collect::<Vec<_>>());
    for chunk_size in [8192, 100_003, 300_001] {
        let mut download = Download::new();
        for chunk in expected.chunks(chunk_size) {
            download = download.append(Bytes::copy_from_slice(chunk)).await?;
            assert!(download.buffer.len() < IO_BUFFER_BYTES);
            assert!(download.buffer.capacity() <= IO_BUFFER_BYTES);
        }
        assert_eq!(download.finish().await?, Payload::from(expected.clone()));
    }
    let mut download = Download::new();
    download = download.append(expected.slice(..17)).await?;
    assert_eq!(
        download.finish().await?,
        Payload::from(expected.slice(..17))
    );
    assert!(Download::new().finish().await?.is_empty());
    Ok(())
}
