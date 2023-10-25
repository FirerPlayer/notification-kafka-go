package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/IBM/sarama"
	"github.com/firerplayer/notification-kafka-go/internal/domain/entity"
	"github.com/gofiber/fiber/v2"
)

const (
	ProducerPort       = ":8080"
	KafkaServerAddress = "localhost:9092"
	KafkaTopic         = "notifications"
)

// ============== HELPER FUNCTIONS ==============
var ErrUserNotFoundInProducer = errors.New("user not found")

func findUserByID(id int, users []entity.User) (entity.User, error) {
	for _, user := range users {
		if user.ID == id {
			return user, nil
		}
	}
	return entity.User{}, ErrUserNotFoundInProducer
}

// ============== KAFKA RELATED FUNCTIONS ==============
func sendKafkaMessage(producer sarama.SyncProducer, message string, fromUser, toUser *entity.User) error {
	notification := entity.Notification{
		From:    *fromUser,
		To:      *toUser,
		Message: message,
	}

	notificationJSON, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to marshal notification: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: KafkaTopic,
		Key:   sarama.StringEncoder(strconv.Itoa(toUser.ID)),
		Value: sarama.StringEncoder(notificationJSON),
	}

	_, _, err = producer.SendMessage(msg)
	return err
}

func sendMessageHandler(producer sarama.SyncProducer, users []entity.User) fiber.Handler {
	return func(ctx *fiber.Ctx) error {
		fromID := ctx.QueryInt("fromID", -1)
		toID := ctx.QueryInt("toID", -1)
		if fromID == -1 || toID == -1 {
			return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{
				"message": "fromID and toID are required",
			})
		}
		user1, err := findUserByID(fromID, users)
		if err != nil {
			return ctx.Status(http.StatusNotFound).JSON(fiber.Map{
				"message": err.Error(),
			})
		}
		user2, err := findUserByID(toID, users)
		if err != nil {
			return ctx.Status(http.StatusNotFound).JSON(fiber.Map{
				"message": err.Error(),
			})
		}

		var mes *string
		err = ctx.BodyParser(&mes)
		if err != nil {
			return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{
				"message": "failed to parse message body",
			})
		}

		err = sendKafkaMessage(producer, *mes, &user1, &user2)
		if err != nil {
			return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{
				"message": err.Error(),
			})
		}

		return ctx.Status(http.StatusOK).JSON(fiber.Map{
			"message": "Notification sent successfully!",
		})
	}
}

func setupProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{KafkaServerAddress}, config)
	if err != nil {
		return nil, fmt.Errorf("failed to setup producer: %w", err)
	}
	return producer, nil
}

func main() {
	users := []entity.User{
		{ID: 1, Name: "Emma"},
		{ID: 2, Name: "Bruno"},
		{ID: 3, Name: "Rick"},
		{ID: 4, Name: "Lena"},
	}

	producer, err := setupProducer()
	if err != nil {
		log.Fatalf("failed to initialize producer: %v", err)
	}
	defer producer.Close()

	router := fiber.New()
	router.Post("/send", sendMessageHandler(producer, users))

	fmt.Printf("Kafka PRODUCER 📨 started at http://localhost%s\n",
		ProducerPort)

	if err := router.Listen(ProducerPort); err != nil {
		log.Printf("failed to run the server: %v", err)
	}
}
