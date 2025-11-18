use tokio::process::ChildStdout;

use crate::event::FfmpegEvent;

async fn process_raw_frames(
  mut stdout: ChildStdout,
  output_streams: Vec<FfmpegOutputStream>,
  outputs: Vec<FfmpegOutput>,
  tx: mpsc::Sender<FfmpegEvent>,
) {
    // Filter streams which are sent to stdout
    let stdout_streams = output_streams.iter().filter(|stream| {
      outputs
        .get(stream.parent_index as usize)
        .map(|o| o.is_stdout())
        .unwrap_or(false)
    });

    // Exit early if nothing is being sent to stdout
    if stdout_streams.clone().count() == 0 {
      tx.send(FfmpegEvent::Error("No streams found".to_owned()))
        .ok();
      return;
    }

    // If the size of a frame can't be determined, it will be read in arbitrary chunks.
    let mut chunked_mode = false;

    // Immediately default to chunked mode for non-video streams
    let stdout_video_streams = stdout_streams.clone().filter(|stream| stream.is_video());
    if stdout_video_streams.clone().count() == 0 {
      chunked_mode = true;
    }

    // Calculate frame buffer sizes up front.
    // Any sizes that cannot be calculated will trigger chunked mode.
    let frame_buffer_sizes: Vec<usize> = stdout_video_streams
      .clone()
      .map(|video_stream| {
        // Any non-rawvideo streams instantly enable chunked mode, since it's
        // impossible to tell when one chunk ends and another begins.
        if video_stream.format != "rawvideo" {
          chunked_mode = true;
          return 0;
        }

        // This is an unexpected error since we've already filtered for video streams.
        let Some(video_data) = video_stream.video_data() else {
          chunked_mode = true;
          return 0;
        };

        // This may trigger either on an unsupported pixel format, or
        // framebuffers with non-byte-aligned sizes. FFmpeg will pad these with
        // zeroes, but we can't predict the exact padding or end size on every format.
        let Some(bytes_per_frame) = get_bytes_per_frame(video_data) else {
          chunked_mode = true;
          return 0;
        };

        bytes_per_frame as usize
      })
      .collect();

    // Final check: FFmpeg supports multiple outputs interleaved on stdout,
    // but we can only keep track of them if the framerates match. It's
    // theoretically still possible to determine the expected frame order,
    // but it's not currently supported.
    let output_framerates: Vec<f32> = stdout_video_streams
      .clone()
      .filter(|s| s.format == "rawvideo")
      .map(|video_stream| {
        if let Some(video_data) = video_stream.video_data() {
          video_data.fps
        } else {
          -1.0
        }
      })
      .collect();
    let any_mismatched_framerates = output_framerates
      .iter()
      .any(|&fps| fps != output_framerates[0] || fps == -1.0);
    if any_mismatched_framerates {
      // This edge case is probably not what the user was intending,
      // so we'll notify with an error.
      tx.send(FfmpegEvent::Error(
        "Multiple output streams with different framerates are not supported when outputting to stdout. Falling back to chunked mode.".to_owned()
      )).ok();
      chunked_mode = true;
    }

    let mut reader = BufReader::new(stdout);
    if chunked_mode {
      // Arbitrary default buffer size for receiving indeterminate chunks
      // of any encoder or container output, when frame boundaries are unknown
      let mut chunk_buffer = vec![0u8; 65_536];
      loop {
        match reader.read(chunk_buffer.as_mut_slice()) {
          Ok(0) => break,
          Ok(bytes_read) => {
            let mut data = vec![0; bytes_read];
            data.clone_from_slice(&chunk_buffer[..bytes_read]);
            tx.send(FfmpegEvent::OutputChunk(data)).ok()
          }
          Err(e) => match e.kind() {
            ErrorKind::UnexpectedEof => break,
            e => tx.send(FfmpegEvent::Error(e.to_string())).ok(),
          },
        };
      }
    } else {
      // Prepare frame buffers
      let mut frame_buffers = frame_buffer_sizes
        .iter()
        .map(|&size| vec![0u8; size])
        .collect::<Vec<Vec<u8>>>();

      // Empty buffer array is unexpected at this point, since we've already ruled out
      // both chunked mode and non-stdout streams.
      if frame_buffers.is_empty() {
        tx.send(FfmpegEvent::Error("No frame buffers found".to_owned()))
          .ok();
        return;
      }

      // Read into buffers
      let num_frame_buffers = frame_buffers.len();
      let mut frame_buffer_index = (0..frame_buffers.len()).cycle();
      let mut frame_num = 0;
      loop {
        let i = frame_buffer_index.next().unwrap();
        let video_stream = &output_streams[i];
        let video_data = video_stream.video_data().unwrap();
        let buffer = &mut frame_buffers[i];
        let output_frame_num = frame_num / num_frame_buffers;
        let timestamp = output_frame_num as f32 / video_data.fps;
        frame_num += 1;

        match reader.read_exact(buffer.as_mut_slice()) {
          Ok(_) => tx
            .send(FfmpegEvent::OutputFrame(OutputVideoFrame {
              width: video_data.width,
              height: video_data.height,
              pix_fmt: video_data.pix_fmt.clone(),
              output_index: i as u32,
              data: buffer.clone(),
              frame_num: output_frame_num as u32,
              timestamp,
            }))
            .ok(),
          Err(e) => match e.kind() {
            ErrorKind::UnexpectedEof => break,
            e => tx.send(FfmpegEvent::Error(e.to_string())).ok(),
          },
        };
      }
    }

    tx.send(FfmpegEvent::Done).ok();
}