import threading
import time
import random
import heapq
from datetime import datetime

CAPACIDADE_FILA_LOCAL = 5
ARQUIVO_LOG = "log_laboratorio_3d.txt"

PRIORIDADES = {
    "Professor": 1,
    "Pós-Graduação": 2,
    "Graduação": 3
}

servidor_central = []
contador_projetos = 1

tipos_iniciais = ["Graduação", "Pós-Graduação", "Professor"]
for _ in range(20):
    tipo = random.choice(tipos_iniciais)
    servidor_central.append((PRIORIDADES[tipo], contador_projetos, tipo))
    contador_projetos += 1

fila_local = []

mutex = threading.Lock()
log_mutex = threading.Lock()

sem_reabastecer = threading.Semaphore(0)
sem_fila_pronta = threading.Semaphore(0)

servidor_ja_acionado = False

total_impresso = 0
total_reabastecimentos = 0
estatisticas = {}
estatisticas_mutex = threading.Lock()

def registrar_log(mensagem):
    instante = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    linha = f"[{instante}] {mensagem}"
    with log_mutex:
        with open(ARQUIVO_LOG, "a", encoding="utf-8") as arquivo:
            arquivo.write(linha + "\n")
        print(linha)

def contabilizar_impressao(nome_usuario):
    global total_impresso
    with estatisticas_mutex:
        total_impresso += 1
        estatisticas[nome_usuario] = estatisticas.get(nome_usuario, 0) + 1

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
                registrar_log(f"[SERVIDOR CENTRAL] Estado Atual da Fila Local (Ordem de Prioridade): {estado_fila}")
            else:
                registrar_log("[SERVIDOR CENTRAL] Alerta: Não há mais arquivos pendentes no Servidor Central!")
            
            servidor_ja_acionado = False
            sem_fila_pronta.release()

def usuario_laboratorio(nome_usuario, tipo_usuario):
    global fila_local, servidor_ja_acionado, servidor_central
    
    while True:
        with mutex:
            registrar_log(f"[{nome_usuario}] Chegou para verificar a fila de impressão.")
            
            if len(fila_local) == 0:
                registrar_log(f"[{nome_usuario}] Viu que a fila local está VAZIA.")
                
                if not servidor_ja_acionado:
                    registrar_log(f"[{nome_usuario}] Acionando o Servidor Central...")
                    servidor_ja_acionado = True
                    sem_reabastecer.release()
                else:
                    registrar_log(f"[{nome_usuario}] Reabastecimento já solicitado por outrem. Aguardando...")
                
                mutex.release()
                sem_fila_pronta.acquire()
                mutex.acquire()
                
        with mutex:
            if len(fila_local) > 0:
                prioridade_num, id_projeto, tipo_projeto = heapq.heappop(fila_local)
                
                registrar_log(f"[{nome_usuario}] Pegou o projeto Doc_{id_projeto} ({tipo_projeto}) para imprimir.")
                
                contabilizar_impressao(nome_usuario)
                registrar_log(f"[{nome_usuario}] Impressora ocupada. Restam {len(fila_local)} arquivos na fila local.")
                
                if len(fila_local) > 0:
                    sem_fila_pronta.release()
            else:
                registrar_log(f"[{nome_usuario}] Fila vazia e servidor sem arquivos. Encerrando turno.")
                if len(servidor_central) == 0 and len(fila_local) == 0:
                    break
                
        time.sleep(random.uniform(1.5, 3.5))

if __name__ == "__main__":
    with open(ARQUIVO_LOG, "w", encoding="utf-8") as f:
        f.write("=== LOG DE OPERAÇÕES - LABORATÓRIO 3D (COM PRIORIDADES REAIS) ===\n")
        
    registrar_log("Iniciando sistema de gerenciamento do laboratório 3D...")
    
    cozinheiro = threading.Thread(target=thread_servidor_central, daemon=True)
    cozinheiro.start()
    
    usuarios = [
        ("Professor 1", "Professor"),
        ("Professor 2", "Professor"),
        ("Aluno Pós 1", "Pós-Graduação"),
        ("Estudante Grad 1", "Graduação"),
        ("Estudante Grad 2", "Graduação"),
        ("Estudante Grad 3", "Graduação")
    ]
    
    threads_usuarios = []
    for nome, tipo in usuarios:
        t = threading.Thread(target=usuario_laboratorio, args=(nome, tipo))
        threads_usuarios.append(t)
        t.start()
        
    for t in threads_usuarios:
        t.join()
        
    registrar_log("\n" + "="*50)
    registrar_log("             RELATÓRIO CONSOLIDADO FINAL             ")
    registrar_log("="*50)
    registrar_log(f"Quantidade total de impressões realizadas : {total_impresso}")
    registrar_log(f"Quantidade de reabastecimentos executados  : {total_reabastecimentos}")
    
    with estatisticas_mutex:
        mais_ativado = max(estatisticas, key=estatisticas.get)
        registrar_log(f"Quem mais utilizou as impressoras         : {mais_ativado} ({estatisticas[mais_ativado]} impressões)")
        registrar_log("Métricas detalhadas por usuário:")
        for usuário, qtd in sorted(estatisticas.items()):
            registrar_log(f"  -> {usuário}: {qtd} impressões concluídas.")
    registrar_log("="*50)