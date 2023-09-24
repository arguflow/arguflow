use crate::data::models::{CutCard, Topic};
use crate::diesel::prelude::*;
use crate::operators::topic_operator::get_topic_query;
use crate::{
    data::models::{Message, Pool},
    errors::DefaultError,
};
use actix_web::web;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatCompletionDTO {
    pub completion_message: Message,
    pub completion_tokens: i32,
}

pub fn get_topic_messages(
    messages_topic_id: uuid::Uuid,
    pool: &web::Data<Pool>,
) -> Result<Vec<Message>, DefaultError> {
    use crate::data::schema::messages::dsl::*;

    let mut conn = pool.get().unwrap();

    let topic_messages = messages
        .filter(topic_id.eq(messages_topic_id))
        .filter(deleted.eq(false))
        .order(sort_order.asc())
        .load::<Message>(&mut conn)
        .map_err(|_db_error| DefaultError {
            message: "Error getting topic messages",
        })?;

    Ok(topic_messages)
}

pub fn user_owns_topic_query(
    user_given_id: uuid::Uuid,
    topic_id: uuid::Uuid,
    pool: &web::Data<Pool>,
) -> Result<Topic, DefaultError> {
    use crate::data::schema::topics::dsl::*;

    let mut conn = pool.get().unwrap();

    let topic: Topic = topics
        .filter(id.eq(topic_id))
        .filter(user_id.eq(user_given_id))
        .first::<crate::data::models::Topic>(&mut conn)
        .map_err(|_db_error| DefaultError {
            message: "Error getting topic",
        })?;

    Ok(topic)
}

pub fn create_message_query(
    new_message: Message,
    given_user_id: uuid::Uuid,
    pool: &web::Data<Pool>,
) -> Result<(), DefaultError> {
    use crate::data::schema::messages::dsl::messages;

    let mut conn = pool.get().unwrap();

    match get_topic_query(new_message.topic_id, pool) {
        Ok(topic) if topic.user_id != given_user_id => {
            return Err(DefaultError {
                message: "Unauthorized",
            })
        }
        Ok(_topic) => {}
        Err(e) => return Err(e),
    };

    diesel::insert_into(messages)
        .values(&new_message)
        .execute(&mut conn)
        .map_err(|_db_error| DefaultError {
            message: "Error creating message, try again",
        })?;

    Ok(())
}

pub fn create_generic_system_message(
    messages_topic_id: uuid::Uuid,
    normal_chat: bool,
    pool: &web::Data<Pool>,
) -> Result<Message, DefaultError> {
    let topic = crate::operators::topic_operator::get_topic_query(messages_topic_id, pool)?;
    let system_message_content = if normal_chat {
        "You are Arguflow Assistant, a large language model trained by Arguflow to be a helpful assistant."
    } else {
        "You are Arguflow retrieval augmented chatbot, a large language model trained by Arguflow to respond in the same tone as and with the context of retrieved information."
    };

    let system_message = Message::from_details(
        system_message_content,
        topic.id,
        0,
        "system".into(),
        Some(0),
        Some(0),
    );

    Ok(system_message)
}

pub fn create_topic_message_query(
    normal_chat: bool,
    previous_messages: Vec<Message>,
    new_message: Message,
    given_user_id: uuid::Uuid,
    pool: &web::Data<Pool>,
) -> Result<Vec<Message>, DefaultError> {
    let mut ret_messages = previous_messages.clone();
    let mut new_message_copy = new_message.clone();
    let mut previous_messages_len = previous_messages.len();

    if previous_messages.is_empty() {
        let system_message =
            create_generic_system_message(new_message.topic_id, normal_chat, pool)?;
        ret_messages.extend(vec![system_message.clone()]);
        create_message_query(system_message, given_user_id, pool)?;
        previous_messages_len = 1;
    }

    new_message_copy.sort_order = previous_messages_len as i32;

    create_message_query(new_message_copy.clone(), given_user_id, pool)?;
    ret_messages.push(new_message_copy);

    Ok(ret_messages)
}

pub fn get_message_by_sort_for_topic_query(
    message_topic_id: uuid::Uuid,
    message_sort_order: i32,
    pool: &web::Data<Pool>,
) -> Result<Message, DefaultError> {
    use crate::data::schema::messages::dsl::*;

    let mut conn = pool.get().unwrap();

    messages
        .filter(deleted.eq(false))
        .filter(topic_id.eq(message_topic_id))
        .filter(sort_order.eq(message_sort_order))
        .first::<Message>(&mut conn)
        .map_err(|_db_error| DefaultError {
            message: "This message does not exist for the authenticated user",
        })
}

pub fn get_messages_for_topic_query(
    message_topic_id: uuid::Uuid,
    pool: &web::Data<Pool>,
) -> Result<Vec<Message>, DefaultError> {
    use crate::data::schema::messages::dsl::*;

    let mut conn = pool.get().unwrap();

    messages
        .filter(topic_id.eq(message_topic_id))
        .filter(deleted.eq(false))
        .order_by(sort_order.asc())
        .load::<Message>(&mut conn)
        .map_err(|_db_error| DefaultError {
            message: "This topic does not exist for the authenticated user",
        })
}

pub fn delete_message_query(
    given_user_id: &uuid::Uuid,
    given_message_id: uuid::Uuid,
    given_topic_id: uuid::Uuid,
    pool: &web::Data<Pool>,
) -> Result<(), DefaultError> {
    use crate::data::schema::messages::dsl::*;

    let mut conn = pool.get().unwrap();

    match get_topic_query(given_topic_id, pool) {
        Ok(topic) if topic.user_id != *given_user_id => {
            return Err(DefaultError {
                message: "Unauthorized",
            })
        }
        Ok(_topic) => {}
        Err(e) => return Err(e),
    };

    let target_message: Message = messages
        .find(given_message_id)
        .first::<Message>(&mut conn)
        .map_err(|_db_error| DefaultError {
            message: "Error finding message",
        })?;

    diesel::update(
        messages
            .filter(topic_id.eq(given_topic_id))
            .filter(sort_order.ge(target_message.sort_order)),
    )
    .set(deleted.eq(true))
    .execute(&mut conn)
    .map_err(|_| DefaultError {
        message: "Error deleting message",
    })?;

    Ok(())
}

pub fn create_cut_card(
    user_id: uuid::Uuid,
    cut_card_content: String,
    pool: web::Data<Pool>,
) -> Result<(), DefaultError> {
    use crate::data::schema::cut_cards::dsl as cut_cards_columns;

    let mut conn = pool.get().map_err(|_| DefaultError {
        message: "Error connecting to database",
    })?;

    let new_cut_card = CutCard::from_details(user_id, cut_card_content);

    diesel::insert_into(cut_cards_columns::cut_cards)
        .values(&new_cut_card)
        .execute(&mut conn)
        .map_err(|_| DefaultError {
            message: "Error creating cut card",
        })?;

    Ok(())
}
