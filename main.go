package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Product struct {
	Name     string
	Category string
}

type Stock struct {
	inventory map[string]chan Product
	mu        sync.Mutex
}

func NewStock() *Stock {
	return &Stock{
		inventory: make(map[string]chan Product),
	}
}

func (s *Stock) AddProduct(product Product) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.inventory[product.Category]; !exists {
		s.inventory[product.Category] = make(chan Product, 100)
	}
	s.inventory[product.Category] <- product
}

func (s *Stock) ConsumeProduct(category string) (Product, int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ch, exists := s.inventory[category]; exists {
		select {
		case product := <-ch:
			remaining := len(ch)
			return product, remaining, true
		default:
			return Product{}, 0, false
		}
	}

	return Product{}, 0, false
}

func (s *Stock) IsEmpty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, ch := range s.inventory {
		if len(ch) > 0 {
			return false
		}
	}
	return true
}

func main() {
	stock := NewStock()

	categories := map[string][]string{
		"Frutas":     {"Maçã", "Banana", "Laranja"},
		"Vegetais":   {"Cenoura", "Batata", "Tomate"},
		"Laticínios": {"Leite", "Queijo", "Iogurte"},
		"Carnes":     {"Frango", "Carne Bovina", "Peixe"},
		"Bebidas":    {"Água", "Suco de Laranja", "Refrigerante"},
	}

	go func() {
		for category, products := range categories {
			for _, product := range products {
				stock.AddProduct(Product{Name: product, Category: category})
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
			}
		}
	}()

	go func() {
		for {
			if stock.IsEmpty() {
				fmt.Println("Todas as categorias estão vazias. Encerrando o consumo.")
				break
			}

			catIndex := rand.Intn(len(categories))
			var category string
			for cat := range categories {
				if catIndex == 0 {
					category = cat
					break
				}
				catIndex--
			}

			if product, remaining, ok := stock.ConsumeProduct(category); ok {
				fmt.Printf("Consumiu: %s da categoria %s. Produtos restantes na categoria: %d\n", product.Name, product.Category, remaining)
			} else {
				fmt.Printf("Categoria %s está vazia\n", category)
			}
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(150)))
		}
	}()

	time.Sleep(5 * time.Second)
}
