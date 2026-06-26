SELECT
  dm.id           AS dm_id,
  dm.content,
  dm.sent_time,
  dm.sender_id,
  dm.receiver_id,
  dm.sent_time,
  dm.message_kind,
  sender.name         AS sender_name,
  receiver.name         AS receiver_name
FROM dm
JOIN users AS sender ON dm.sender_id = sender.id
JOIN users AS receiver ON dm.receiver_id = receiver.id
WHERE (dm.sender_id = 'd1b8dacc-9b9c-499f-ae56-a73c270567d2' AND dm.receiver_id = '6f79714e-e3f6-4a8a-9c44-9cbe8bb2a45c') OR (dm.sender_id = '6f79714e-e3f6-4a8a-9c44-9cbe8bb2a45c' AND dm.receiver_id = 'd1b8dacc-9b9c-499f-ae56-a73c270567d2')
ORDER BY dm.sent_time DESC LIMIT 20;
