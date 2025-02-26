use itertools::Itertools;
use ndarray::Array2;
use regex::Regex;
use regex_split::RegexSplit;
use scraper::{Html, Selector};
use std::{
    cmp,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc, Mutex,
    },
};

use crate::errors::ServiceError;

pub fn convert_html_to_text(html: &str) -> String {
    let dom = tl::parse(html, tl::ParserOptions::default()).unwrap();
    let parser = dom.parser();

    let text = dom
        .nodes()
        .iter()
        .map(|node| node.inner_text(parser))
        .join("\n");

    text
}

pub fn extract_text_from_html(html: &str) -> String {
    let document = Html::parse_document(html);
    let selector = Selector::parse("body").unwrap();

    document
        .select(&selector)
        .flat_map(|element| element.text())
        .map(|text| text.trim())
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn coarse_remove_large_chunks(cur_chunks: Vec<String>) -> Vec<String> {
    let max_chunk_len = 10000;
    let mut chunks = cur_chunks;
    let mut new_chunks: Vec<String> = vec![];

    for chunk in chunks.iter_mut() {
        let char_count = chunk.chars().count() as f32;

        if char_count < (max_chunk_len as f32) {
            new_chunks.push(chunk.to_string());
            continue;
        }

        let num_new_chunks = (char_count / max_chunk_len as f32).ceil() as usize;
        let chunk_size = (char_count / num_new_chunks as f32).ceil();
        let mut total_length = char_count;

        while total_length > 0.0 {
            let amt_to_remove = cmp::min(chunk_size as usize, total_length as usize);
            let mut amt_to_take = amt_to_remove;
            while !chunk.is_char_boundary(amt_to_take) {
                amt_to_take -= 1;
            }

            let new_chunk = chunk.chars().take(amt_to_take).collect::<String>();
            new_chunks.push(new_chunk);

            amt_to_take = cmp::min(amt_to_take, chunk.chars().count());
            chunk.drain(0..amt_to_take);
            total_length -= amt_to_remove as f32;
        }
    }

    new_chunks.retain(|x| !x.is_empty());
    new_chunks
}

pub fn build_chunking_regex(delimiters: Vec<String>) -> Result<Regex, regex::Error> {
    let escaped_delimiters: Vec<String> = delimiters.iter().map(|x| regex::escape(x)).collect();
    let pattern = escaped_delimiters.join("|");
    let re = Regex::new(&pattern)?;
    Ok(re)
}

pub fn coarse_doc_chunker(
    document: String,
    split_pattern: Option<Regex>,
    rebalance_chunks: bool,
    target_splits_per_chunk: usize,
) -> Vec<String> {
    log::info!("Starting to parse fragment");
    let dom = Html::parse_fragment(&document);
    log::info!("Coarse doc chunker parsed HTML for the document");
    let clean_text = dom.root_element().text().collect::<String>();
    log::info!("Coarse doc chunker extracted text from HTML");

    let pattern = match split_pattern {
        Some(pattern) => pattern,
        None => build_chunking_regex(vec![".".to_string(), "?".to_string(), "\\n".to_string()])
            .expect("regex is always correct"),
    };

    let mut splits = pattern
        .split_inclusive(&clean_text)
        .filter(|split| split.len() > 2);
    let mut splits_count = pattern
        .split_inclusive(&clean_text)
        .filter(|split| split.len() > 2)
        .count();

    log::info!("Coarse doc chunker created {} splits", splits_count);

    let mut groups: Vec<String> = vec![];
    if splits_count < target_splits_per_chunk {
        groups.push(splits.join(""));
        log::info!("Document is too small to chunk. Returning the entire document as a single chunk after removing any large outliers.");
        return coarse_remove_large_chunks(groups);
    }

    let mut remainder = (splits_count % target_splits_per_chunk) as f32;
    let group_count = ((splits_count / target_splits_per_chunk) as f32).floor();
    let remainder_per_group = (remainder / group_count).ceil();

    if rebalance_chunks {
        while remainder > 0.0 {
            let group_size = cmp::min(
                target_splits_per_chunk
                    + cmp::min(remainder as usize, remainder_per_group as usize),
                splits_count,
            );
            let group = splits.by_ref().take(group_size).join("");

            groups.push(group);
            splits_count -= group_size;
            remainder -= remainder_per_group;
        }
    }

    while splits_count > 0 {
        let drain_amt = cmp::min(target_splits_per_chunk, splits_count);

        let group = splits
            .by_ref()
            .take(drain_amt)
            .collect::<Vec<&str>>()
            .join("");
        groups.push(group);
        splits_count -= drain_amt;
    }

    coarse_remove_large_chunks(groups)
}

pub fn average_embeddings(embeddings: Vec<Vec<f32>>) -> Result<Vec<f32>, ServiceError> {
    let first_embedding_len = match embeddings.first() {
        Some(embedding) => embedding.len(),
        None => {
            return Err(ServiceError::BadRequest(
                "No embeddings provided".to_string(),
            ));
        }
    };

    let shape = (embeddings.len(), first_embedding_len);
    let flat: Vec<f32> = embeddings.iter().flatten().cloned().collect();
    let arr: Array2<f32> = Array2::from_shape_vec(shape, flat).map_err(|e| {
        log::error!("Error creating ndarray from embeddings: {}", e);
        ServiceError::InternalServerError(
            "Error creating ndarray from embeddings to average".to_string(),
        )
    })?;

    Ok((arr.sum_axis(ndarray::Axis(0)) / (embeddings.len() as f32)).to_vec())
}

/// Parse the response from the LLM server
///
/// State Table
/// 0 - Unkown
/// 1 - Documents
/// 2 - Message
pub fn parse_streaming_completetion(
    response: &str,
    state: Arc<AtomicU16>,
    documents: Arc<Mutex<Vec<u32>>>,
) -> (Option<String>, Option<Vec<u32>>, bool) {
    const STATE_INITIAL: u16 = 0;
    const STATE_COLLECTING: u16 = 1;
    const STATE_COMPLETE: u16 = 2;

    match state.load(Ordering::Relaxed) {
        STATE_COMPLETE => (Some(response.into()), None, false),
        STATE_INITIAL if response.bytes().any(|b| matches!(b, b'd' | b'D')) => {
            state.store(STATE_COLLECTING, Ordering::Relaxed);
            (None, None, false)
        }
        STATE_COLLECTING => {
            if response.as_bytes().contains(&b']') {
                state.store(STATE_COMPLETE, Ordering::Relaxed);
                (None, Some(documents.lock().unwrap().clone()), false)
            } else if response.bytes().all(|b| b.is_ascii_digit()) {
                if let Ok(num) = response.parse::<u32>() {
                    documents.lock().unwrap().push(num - 1);
                }
                (None, None, false)
            } else {
                (None, None, false)
            }
        }
        _ if response.trim().chars().all(|c| !c.is_alphanumeric()) => (None, None, false),
        _ => (None, None, true),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn test_average_embeddings() {
        let embeddings = vec![
            vec![3.0, 2.5, 1.0],
            vec![1.0, 2.5, 1.0],
            vec![2.0, 2.5, 1.0],
            vec![2.0, 2.5, 1.0],
        ];

        let result = average_embeddings(embeddings).unwrap();
        assert!(result == vec![2.0, 2.5, 1.0]);
    }
}
