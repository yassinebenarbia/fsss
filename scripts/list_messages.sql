SELECT messages.id, messages.space_id, messages.content, messages.sender_id, messages.message_kind, messages.sent_time, users.name, replies.replying_to FROM messages 
INNER JOIN users ON users.id = messages.sender_id   AND messages.space_id = '6e7db902-997e-4068-833e-be8e9b7f7f58'
LEFT JOIN message_metadata ON messages.metadata = message_metadata.id 
LEFT JOIN replies ON message_metadata.reply = replies.id 
ORDER BY messages.sent_time DESC LIMIT 20;
