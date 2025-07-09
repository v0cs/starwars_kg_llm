from llm.chain import StarWarsQAChain
import argparse

def display_result(result):
    """Exibe os resultados formatados"""
    if "error" in result:
        print(f"\n❌ Erro: {result['error']}")
    else:
        print("\n🔵 Resposta:", result["answer"])
        print("\n🔷 Consulta Cypher:", result["cypher_query"])
        if "context" in result:
            print("\n📌 Contexto:", result["context"])

def main():
    try:
        qa_system = StarWarsQAChain()
        
        parser = argparse.ArgumentParser(
            description="Sistema de Q&A do Universo Star Wars",
            formatter_class=argparse.RawTextHelpFormatter
        )
        parser.add_argument(
            "--question", 
            type=str, 
            help="Pergunta sobre o universo Star Wars\nExemplo: 'Quem são os personagens do Episódio IV?'"
        )
        args = parser.parse_args()
        
        if args.question:
            result = qa_system.query(args.question)
            display_result(result)
        else:
            print("💫 Sistema de Q&A do Universo Star Wars")
            print("Digite 'sair' para terminar\n")
            while True:
                question = input("\n🌌 Pergunta: ").strip()
                if question.lower() in ['sair', 'exit']:
                    break
                if not question:
                    continue
                    
                result = qa_system.query(question)
                display_result(result)
                
    except Exception as e:
        print(f"\n⚠️ Erro crítico: {str(e)}")
        print("Verifique:\n1. Se o Neo4j está rodando\n2. Se o Ollama está disponível\n3. As configurações no arquivo .env")

if __name__ == "__main__":
    main()