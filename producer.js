const { Kafka } = require('kafkajs')

const run = async () => {
  // Producing
  const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["localhost:9092"],
  });

  const producer = kafka.producer()
  await producer.connect()
  await producer.send({
    topic: 'user',
    messages: [
      {
        key : "1710",
        headers : {
          'event' : 'level_up'
        },
        value: JSON.stringify({
          "id": "616d3fea02aa1b001d0e6963",
          "event": "level_up",
          "topic": "user",
          "key": "1710",
          "subject": {
            "id": "1081",
            "name": "ti tach 4",
            "type": "mod"
          },
          "di_obj": {
            "type": "User",
            "id": "1710",
            "data": {
              "name": "Billy",
              "full_name": "Billy",
              "status": 1,
              "gender": 1,
              "birthday": "1999-09-10",
              "avatar": "",
              "last_kicked_at": null,
              "kicked_count": 0,
              "country" : "VN",
              "is_user_password": true,
              "level": 5803,
              "room_count": 17,
              "room_level_count": 4,
              "room_talk_count": 16,
              "room_listen_count": 1,
              "rated_at": "2021-10-18T09:30:34.586Z",
              "self_rated_at": null,
              "paid_session_count": 96,
              "next_action": "",
              "room_ticket_count": 66,
              "newest_rating_score": 4.5,
              "level_up_action": 1,
              "_id": 1710,
              "password": "$2a$04$zq6q/eHDG5pz/EQGXQbxbuA50b/S/QgvrysXyUtcp6BaOBx.1FbQm",
              "phone": "+84828504991",
              "is_validated_phone": true,
              "created_at": "2021-09-09T13:43:00.890Z",
              "updated_at": "2021-10-18T09:31:46.826Z",
              "__v": 0
            }
          },
          "pr_obj": {
            "type": "UserLevelHistory",
            "id": "616d3eba62477ab81f3364f4",
            "data": {
              "id": "616d3eba62477ab81f3364f4",
              "user_id": 1710,
              "level": 5803,
              "level_up_action": 1,
              "level_name": "Kid Flyers 3",
              "moderator_id": 1081,
              "moderator_name": "ti tach 4",
              "room_id": "616d3e4d02aa1b001d0e6959",
              "rating": {
                "quick_assessment": {
                  "good_at": {
                    "other_comment": "",
                    "comments": [
                      {
                        "content": {
                          "vi": "Can use full range of pronunciation features",
                          "en": "Can use full range of pronunciation features"
                        },
                        "id": "6120be409c30c12d691e5924"
                      },
                      {
                        "content": {
                          "vi": "Understand Effortlessly",
                          "en": "Understand Effortlessly"
                        },
                        "id": "6120be409c30c12d691e5925"
                      },
                      {
                        "content": {
                          "vi": "Use idiomatic language naturally and accurately",
                          "en": "Use idiomatic language naturally and accurately"
                        },
                        "id": "6120be409c30c12d691e5926"
                      },
                      {
                        "content": {
                          "vi": "Use paraphrase effectively as required",
                          "en": "Use paraphrase effectively as required"
                        },
                        "id": "6120be409c30c12d691e5927"
                      }
                    ]
                  },
                  "need_to_improve": {
                    "other_comment": "",
                    "comments": [
                      {
                        "content": {
                          "vi": "Need to increase the understanding of grammatical rules and constructions",
                          "en": "Need to increase the understanding of grammatical rules and constructions"
                        },
                        "id": "6120be409c30c12d691e7924"
                      },
                      {
                        "content": {
                          "vi": "Should practise using the structures correctly in conversation",
                          "en": "Should practise using the structures correctly in conversation"
                        },
                        "id": "6120be409c30c12d691e7925"
                      },
                      {
                        "content": {
                          "vi": "Need to broaden your vocabulary to enable you to cope in a greater variety of contexts",
                          "en": "Need to broaden your vocabulary to enable you to cope in a greater variety of contexts"
                        },
                        "id": "6120be409c30c12d691e7926"
                      }
                    ]
                  }
                },
                "praise_assessment": {
                  "content": {
                    "vi": "It’s not about having time. It’s about making time",
                    "en": "It’s not about having time. It’s about making time"
                  },
                  "id": "6163ac26de79cd31a68b5b35"
                },
                "periodical_assessment": [
                  {
                    "name": {
                      "vi": "Pronunciation",
                      "en": "Pronunciation"
                    },
                    "other_comment": null,
                    "group": "pronunciation",
                    "score": 5,
                    "comments": []
                  },
                  {
                    "name": {
                      "vi": "Vocab",
                      "en": "Vocab"
                    },
                    "other_comment": null,
                    "group": "vocab",
                    "score": 4,
                    "comments": []
                  },
                  {
                    "name": {
                      "vi": "Grammar",
                      "en": "Grammar"
                    },
                    "other_comment": null,
                    "group": "grammar",
                    "score": 5,
                    "comments": []
                  },
                  {
                    "name": {
                      "vi": "Fluency",
                      "en": "Fluency"
                    },
                    "other_comment": null,
                    "group": "fluency",
                    "score": 4,
                    "comments": []
                  }
                ],
                "rating_type": 3,
                "average_score": 4.5
              },
              "created_at": 1634549434
            }
          },
          "sent_at": 1634549738646
        })
      },
    ],
  })
}

run().catch(console.error)