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
        """Implementação manual robusta para geração de Cypher"""
        from langchain.prompts import PromptTemplate
        
        # Template melhorado para Cypher
        cypher_template = """Você é um especialista em Neo4j Cypher. 
        Gere SOMENTE a consulta Cypher para: {question}
        Use apenas os seguintes padrões de relacionamento: -[:APARECE_EM]->
        Retorne APENAS a consulta Cypher, sem explicações ou texto adicional.
        
        Exemplo para "Quem são os personagens de Star Wars?":
        MATCH (p:Personagem) RETURN p.nome LIMIT 10
        
        Consulta Cypher:"""
        
        self.cypher_prompt = PromptTemplate(
            template=cypher_template,
            input_variables=["question"]
        )
        
        # Template para resposta final
        answer_template = """Responda à pergunta baseada nos resultados:
        Pergunta: {question}
        Dados: {results}
        Resposta:"""
        
        self.answer_prompt = PromptTemplate(
            template=answer_template,
            input_variables=["question", "results"]
        )
    
    def test_connections(self):
        try:
            result = self.graph.query(
                "MATCH (n) RETURN count(n) AS node_count LIMIT 1"
            )
            print(f"✅ Neo4j conectado - Nós: {result[0]['node_count']}")
            
            test_response = self.llm.invoke("Teste")
            print(f"✅ Ollama conectado - Resposta: {test_response[:50]}...")
            
            return True
        except Exception as e:
            print(f"❌ Erro de conexão: {str(e)}")
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
        """Executa consulta usando implementação manual melhorada"""
        try:
            # Gera consulta Cypher
            cypher_query = self.llm.invoke(
                self.cypher_prompt.format(question=question)
            ).strip()
            
            print(f"Generated Cypher: {cypher_query}")  # Debug
            
            # Executa no Neo4j
            results = self.graph.query(cypher_query)
            
            # Gera resposta final
            answer = self.llm.invoke(
                self.answer_prompt.format(
                    question=question,
                    results=str(results)
                )
            )
            
            return {
                "answer": answer,
                "cypher_query": cypher_query,
                "context": results
            }
            
        except Exception as e:
            return {"error": f"Erro: {str(e)}"}

    def get_schema(self):
        """Método para visualizar o esquema do banco"""
        try:
            return self.graph.get_schema
        except Exception as e:
            return f"Erro ao obter esquema: {str(e)}"