package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Message representa uma mensagem na fila, contendo um conteúdo e um tópico.
type Message struct {
	Content string // Conteúdo da mensagem
	Topic   string // Tópico ao qual a mensagem pertence
}

// MessageQueue é uma estrutura thread-safe para gerenciar filas de mensagens baseadas em tópicos.
type MessageQueue struct {
	queues map[string]chan Message // Mapa de nomes de tópicos para seus respectivos canais de mensagens
	mu     sync.Mutex              // Mutex para proteger o acesso concorrente ao mapa
}

// NewMessageQueue inicializa uma nova MessageQueue com um mapa vazio de filas.
func NewMessageQueue() *MessageQueue {
	return &MessageQueue{
		queues: make(map[string]chan Message),
	}
}

// Publish adiciona uma mensagem à fila do tópico especificado. Se o tópico não existir, cria um novo canal.
func (mq *MessageQueue) Publish(message Message) {
	mq.mu.Lock()         // Trava o mapa para garantir acesso seguro em concorrência
	defer mq.mu.Unlock() // Garante que o mutex seja destravado após a operação

	// Verifica se o tópico já possui uma fila
	ch, exists := mq.queues[message.Topic]
	if !exists {
		// Cria um novo canal para o tópico se ele não existir
		ch = make(chan Message, 10) // Tamanho do buffer de 10; pode ser ajustado
		mq.queues[message.Topic] = ch
	}

	// Envia a mensagem para o canal do tópico
	ch <- message
}

// Subscribe retorna um canal que escuta mensagens do tópico especificado. Cria um novo canal se o tópico não existir.
func (mq *MessageQueue) Subscribe(topic string) <-chan Message {
	mq.mu.Lock()         // Trava o mapa para garantir acesso seguro em concorrência
	defer mq.mu.Unlock() // Garante que o mutex seja destravado após a operação

	// Verifica se o tópico já possui uma fila
	ch, exists := mq.queues[topic]
	if !exists {
		// Cria um novo canal para o tópico se ele não existir
		ch = make(chan Message, 10) // Tamanho do buffer de 10; pode ser ajustado
		mq.queues[topic] = ch
	}

	// Retorna o canal para o tópico
	return ch
}

// Producer simula um produtor de mensagens que publica mensagens em tópicos aleatórios.
func Producer(mq *MessageQueue, topics []string, wg *sync.WaitGroup) {
	defer wg.Done() // Decrementa o contador do WaitGroup quando a função termina

	for {
		// Seleciona um tópico aleatório da lista
		topic := topics[rand.Intn(len(topics))]

		// Cria uma nova mensagem com o tópico selecionado
		message := Message{
			Content: fmt.Sprintf("Mensagem no %s às %d", topic, time.Now().Unix()),
			Topic:   topic,
		}

		// Publica a mensagem na fila
		mq.Publish(message)

		// Loga a mensagem publicada
		fmt.Println("Publicado:", message.Content)

		// Simula algum trabalho com um sleep
		time.Sleep(time.Second)
	}
}

// Consumer simula um consumidor de mensagens que processa mensagens de um tópico específico.
func Consumer(mq *MessageQueue, topic string, wg *sync.WaitGroup) {
	defer wg.Done() // Decrementa o contador do WaitGroup quando a função termina

	// Inscreve-se no canal de mensagens do tópico
	ch := mq.Subscribe(topic)

	// Processa as mensagens conforme elas chegam
	for message := range ch {
		fmt.Printf("Consumidor no %s consumiu: %s\n", topic, message.Content)
	}
}

func main() {
	// Inicializa a fila de mensagens
	mq := NewMessageQueue()

	// Lista de tópicos a serem usados no sistema pub-sub
	topics := []string{"topic1", "topic2", "topic3", "topic4", "topic5", "topic6", "topic7", "topic8"}
	var wg sync.WaitGroup

	// Inicia múltiplos produtores
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go Producer(mq, topics, &wg)
	}

	// Inicia múltiplos consumidores, alternando entre os tópicos
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go Consumer(mq, topics[i%len(topics)], &wg)
	}

	// Aguarda todas as goroutines terminarem
	wg.Wait()
}
