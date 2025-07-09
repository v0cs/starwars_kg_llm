from langchain_neo4j import Neo4jGraph
from langchain_ollama import OllamaLLM
from dotenv import load_dotenv
import os

load_dotenv()

class StarWarsQAChain:
    def __init__(self):
        """Inicialização com compatibilidade para diferentes versões do LangChain"""
        try:
            self.graph = Neo4jGraph(
                url=os.getenv("NEO4J_URI"),
                username=os.getenv("NEO4J_USERNAME"),
                password=os.getenv("NEO4J_PASSWORD"),
                sanitize=True
            )
            
            self.llm = OllamaLLM(
                model=os.getenv("OLLAMA_MODEL"),
                base_url=os.getenv("OLLAMA_BASE_URL"),
                temperature=0.3,
                top_k=40,
                top_p=0.9
            )
            
            self.chain = None
            self._setup_chain()
            
            print("✅ Inicialização concluída com sucesso!")
            
        except Exception as e:
            raise RuntimeError(f"Falha na inicialização: {str(e)}")
    
    def _setup_chain(self):
        """Configura a cadeia de QA tentando diferentes importações"""
        try:
            from langchain.chains import GraphCypherQAChain
            self.chain = GraphCypherQAChain.from_llm(
                llm=self.llm,
                graph=self.graph,
                verbose=True,
                validate_cypher=True,
                return_intermediate_steps=True,
                top_k=10
            )
            print("✅ Usando langchain.chains.GraphCypherQAChain")
            return
        except Exception as e:
            print(f"⚠️ Tentativa 1 falhou: {str(e)}")
        
        try:
            from langchain_community.chains.graph_qa.cypher import GraphCypherQAChain
            self.chain = GraphCypherQAChain.from_llm(
                llm=self.llm,
                graph=self.graph,
                verbose=True,
                validate_cypher=True,
                return_intermediate_steps=True,
                top_k=10
            )
            print("✅ Usando langchain_community.chains.graph_qa.cypher.GraphCypherQAChain")
            return
        except Exception as e:
            print(f"⚠️ Tentativa 2 falhou: {str(e)}")
        
        # Tentativa 3: Implementação manual básica
        try:
            self._create_manual_chain()
            print("✅ Usando implementação manual")
            return
        except Exception as e:
            print(f"⚠️ Tentativa 3 falhou: {str(e)}")
            
        raise RuntimeError("Não foi possível configurar a cadeia de QA")
    
    def _create_manual_chain(self):
        """Cria uma implementação manual básica da cadeia"""
        from langchain.prompts import PromptTemplate
        from langchain.chains import LLMChain
        
        cypher_template = """
        Você é um especialista em Cypher para Neo4j. Baseado na pergunta do usuário, 
        gere uma consulta Cypher para um banco de dados sobre Star Wars.
        
        Esquema do banco:
        {schema}
        
        Pergunta: {question}
        
        Consulta Cypher:
        """
        
        self.cypher_prompt = PromptTemplate(
            template=cypher_template,
            input_variables=["schema", "question"]
        )
        
        answer_template = """
        Baseado na consulta Cypher executada e nos resultados obtidos, 
        responda à pergunta do usuário de forma clara e concisa.
        
        Pergunta: {question}
        Consulta Cypher: {cypher_query}
        Resultados: {results}
        
        Resposta:
        """
        
        self.answer_prompt = PromptTemplate(
            template=answer_template,
            input_variables=["question", "cypher_query", "results"]
        )
        
        self.cypher_chain = LLMChain(llm=self.llm, prompt=self.cypher_prompt)
        self.answer_chain = LLMChain(llm=self.llm, prompt=self.answer_prompt)
    
    def test_connections(self):
        """Método para testar as conexões antes de usar"""
        try:
            # Teste Neo4j
            result = self.graph.query("MATCH (n) RETURN count(n) as total_nodes LIMIT 1")
            print(f"✅ Neo4j conectado - Total de nós: {result[0]['total_nodes']}")
            
            # Teste Ollama
            test_response = self.llm.invoke("Teste de conexão")
            print(f"✅ Ollama conectado - Resposta: {test_response[:50]}...")
            
            return True
        except Exception as e:
            print(f"❌ Erro na conexão: {str(e)}")
            return False

    def query(self, question):
        """Método para executar consultas com diferentes implementações"""
        try:
            print(f"🔍 Processando pergunta: {question}")
            
            if hasattr(self, 'chain') and self.chain:
                return self._query_with_chain(question)
            else:
                return self._query_manual(question)
                
        except Exception as e:
            return {"error": f"Erro na consulta: {str(e)}"}
    
    def _query_with_chain(self, question):
        """Executa consulta usando a cadeia configurada"""
        result = self.chain.invoke({"question": question})
        return {
            "answer": result.get("result", "Nenhuma resposta encontrada"),
            "cypher_query": result.get("intermediate_steps", [{}])[0].get("query", "Consulta não disponível") if result.get("intermediate_steps") else "Consulta não disponível",
            "context": result.get("context", [])
        }
    
    def _query_manual(self, question):
        """Executa consulta usando implementação manual"""
        schema = self.graph.get_schema
        
        cypher_response = self.cypher_chain.invoke({
            "schema": schema,
            "question": question
        })
        
        cypher_query = cypher_response["text"].strip()
        
        try:
            results = self.graph.query(cypher_query)
            
            answer_response = self.answer_chain.invoke({
                "question": question,
                "cypher_query": cypher_query,
                "results": str(results)
            })
            
            return {
                "answer": answer_response["text"].strip(),
                "cypher_query": cypher_query,
                "context": results
            }
            
        except Exception as e:
            return {"error": f"Erro ao executar consulta Cypher: {str(e)}"}

    def get_schema(self):
        """Método para visualizar o esquema do banco"""
        try:
            return self.graph.get_schema
        except Exception as e:
            return f"Erro ao obter esquema: {str(e)}"