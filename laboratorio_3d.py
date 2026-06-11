import threading
import time
import random
import heapq
from datetime import datetime

CAPACIDADE_FILA_LOCAL = 5
NUM_ESTUDANTES = 5
ARQUIVO_LOG = "log_laboratorio_3d.txt"

PRIORIDADES = {
    "Professor": 1,
    "Pós-Graduação": 2,
    "Graduação": 3
}

NOME_PRIORIDADE = {v: k for k, v in PRIORIDADES.items()}

servidor_central = []
contador_projetos = 1

tipos_projetos = ["Graduação", "Pós-Graduação", "Professor"]
for _ in range(30):
    tipo = random.choice(tipos_projetos)
    prioridade_num = PRIORIDADES[tipo]
    servidor_central.append((prioridade_num, contador_projetos, tipo))
    contador_projetos += 1

fila_local = []

mutex = threading.Lock()
log_mutex = threading.Lock()

sem_reabastecer = threading.Semaphore(0)
sem_fila_pronta = threading.Semaphore(0)

servidor_ja_acionado = False

total_impresso = 0
total_reabastecimentos = 0
estatisticas_estudantes = {i: 0 for i in range(1, NUM_ESTUDANTES + 1)}

def registrar_log(mensagem):
    instante = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    linha = f"[{instante}] {mensagem}"
    with log_mutex:
        with open(ARQUIVO_LOG, "a", encoding="utf-8") as arquivo:
            arquivo.write(linha + "\n")
        print(linha)

def thread_servidor_central():
    global fila_local, servidor_ja_acionado, total_reabastecimentos, servidor_central
    
    while True:
        sem_reabastecer.acquire()
        
        with mutex:
            registrar_log("[SERVIDOR CENTRAL] Solicitação recebida. Reabastecendo fila local...")
            
            vagas_disponiveis = CAPACIDADE_FILA_LOCAL - len(fila_local)
            quantidade_a_trazer = min(vagas_disponiveis, len(servidor_central))
            
            if quantidade_a_trazer > 0:
                servidor_central.sort()
                
                for _ in range(quantidade_a_trazer):
                    projeto = servidor_central.pop(0)
                    heapq.heappush(fila_local, projeto)
                
                total_reabastecimentos += 1
                registrar_log(f"[SERVIDOR CENTRAL] Reabastecimento concluído. Foram adicionados {quantidade_a_trazer} arquivos.")
                estado_fila = [f"Doc_{p[1]}({p[2]})" for p in sorted(fila_local)]
                registrar_log(f"[SERVIDOR CENTRAL] Estado da Fila Local: {estado_fila}")
            else:
                registrar_log("[SERVIDOR CENTRAL] Alerta: Não há mais arquivos pendentes no Servidor Central!")
            
            servidor_ja_acionado = False
            sem_fila_pronta.release()

def thread_estudante(id_estudante):
    global fila_local, servidor_ja_acionado, total_impresso
    
    while True:
        with mutex:
            registrar_log(f"[ESTUDANTE {id_estudante}] Deseja realizar uma impressão.")
            
            if len(fila_local) == 0:
                registrar_log(f"[ESTUDANTE {id_estudante}] Identificou que a fila local está VAZIA.")
                
                if not servidor_ja_acionado:
                    registrar_log(f"[ESTUDANTE {id_estudante}] Acionando o Servidor Central para reabastecimento.")
                    servidor_ja_acionado = True
                    sem_reabastecer.release()
                else:
                    registrar_log(f"[ESTUDANTE {id_estudante}] O reabastecimento já foi solicitado por outro usuário. Aguardando...")
                
                mutex.release()
                sem_fila_pronta.acquire()
                mutex.acquire()
                
        with mutex:
            if len(fila_local) > 0:
                prioridade_num, id_projeto, tipo = heapq.heappop(fila_local)
                
                registrar_log(f"[ESTUDANTE {id_estudante}] INICIOU a impressão do projeto Doc_{id_projeto} | Prioridade: {tipo}.")
                
                total_impresso += 1
                estatisticas_estudantes[id_estudante] += 1
                registrar_log(f"[ESTUDANTE {id_estudante}] Restam {len(fila_local)} arquivos na fila local.")
                
                if len(fila_local) > 0:
                    sem_fila_pronta.release()
            else:
                registrar_log(f"[ESTUDANTE {id_estudante}] Não encontrou arquivos adicionais e retornará mais tarde.")
                if len(servidor_central) == 0 and len(fila_local) == 0:
                    break
                
        time.sleep(random.uniform(1.0, 3.0))

if __name__ == "__main__":
    with open(ARQUIVO_LOG, "w", encoding="utf-8") as f:
        f.write("=== LOG DE OPERAÇÕES - GERENCIADOR DE IMPRESSÃO 3D ===\n")
        
    registrar_log("Iniciando o sistema de escalonamento do laboratório...")
    
    thread_servidor = threading.Thread(target=thread_servidor_central, daemon=True)
    thread_servidor.start()
    
    threads_estudantes = []
    for i in range(1, NUM_ESTUDANTES + 1):
        t = threading.Thread(target=thread_estudante, args=(i,))
        threads_estudantes.append(t)
        t.start()
        
    for t in threads_estudantes:
        t.join()
        
    registrar_log("\n" + "="*50)
    registrar_log("             RELATÓRIO CONSOLIDADO FINAL             ")
    registrar_log("="*50)
    registrar_log(f"Quantidade total de impressões realizadas : {total_impresso}")
    registrar_log(f"Quantidade de reabastecimentos executados  : {total_reabastecimentos}")
    
    maior_consumidor = max(estatisticas_estudantes, key=estatisticas_estudantes.get)
    registrar_log(f"Estudante que mais imprimiu trabalhos     : Estudante {maior_consumidor} ({estatisticas_estudantes[maior_consumidor]} impressões)")
    registrar_log("Métricas detalhadas por usuário:")
    for est, qtd in estatisticas_estudantes.items():
        registrar_log(f"  -> Estudante {est}: {qtd} impressões concluídas.")
    registrar_log("="*50)