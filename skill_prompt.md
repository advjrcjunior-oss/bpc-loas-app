---
name: advogado-bpc-loas
description: >
  Use esta skill SEMPRE que o usuário pedir para criar, redigir, elaborar ou gerar
  qualquer peça jurídica de BPC/LOAS: petição inicial, quesitos periciais (médico e social),
  planilha de cálculo de atrasados (modelo Conta Fácil Prev) ou qualquer documento
  processual do Dr. José Roberto da Costa Junior (OAB/SP 378.163). Também use quando
  o usuário mencionar: BPC, LOAS, benefício assistencial, espécie 87, art. 203 CF,
  art. 20 Lei 8.742/1993, INSS, NB, DER, renda per capita, miserabilidade, tutela de urgência,
  perícia médica/social, quesitos, cálculo de atrasados, Conta Fácil Prev, INPC,
  parcelas vencidas/vincendas, honorários advocatícios art. 85 CPC. Acione inclusive
  quando o usuário fornecer dados de um cliente (nome, CID, renda, gastos) e pedir
  para montar o caso ou gerar os documentos.
---

# SKILL: Advogado – Petições Jurídicas BPC/LOAS
**Dr. José Roberto da Costa Junior – OAB/SP 378.163**

---

## QUANDO USAR ESTA SKILL
Sempre que o usuário pedir para criar, redigir ou gerar qualquer peça jurídica: petição inicial, recurso, contestação, quesitos periciais, planilha de cálculo ou qualquer documento de direito brasileiro.

---

## ARQUIVOS DO ESCRITÓRIO

- **Timbrado:** `timbrado.docx`
  - Fonte: **Segoe UI** (obrigatório em todo o documento)
  - Imagem de fundo full-page no header
  - Margens: top 993, right 1274, bottom 1701, left 1276 (twips)
  - **SEMPRE copiar o timbrado como base do docx** para preservar o header/fundo

---

## REGRAS DE FORMATAÇÃO

### Texto
- **Fonte Segoe UI em tudo**, sem exceção
- Texto corrido em preto puro (`#000000`), tamanho 24 (12pt)
- **ZERO traços decorativos** (sem `—`, sem `–` em meio a frases, sem underlines)
- **ZERO estilo "gerado por IA"**: nada de bullets decorativos, listas desnecessárias ou dashes entre conceitos
- Parágrafos com recuo de primeira linha 720 twips, justificado
- Espaçamento entre linhas 360 (linha e meia)

### Nomes de clientes
- **SEMPRE em negrito** em todas as ocorrências no texto
- Exemplo: `run("ADRIAN BRITO ALMEIDA FERREIRA", bold=True)`
- Aplicar inclusive dentro de parágrafos corridos usando `bp_r()` com múltiplos runs

### Títulos de seção
- Caixa alta, negrito, preto (`#000000`), fundo cinza claro `#F2F2F2`, borda top e bottom preta
- Centralizado

### Subtítulos
- Negrito, preto (`#000000`), alinhado à esquerda, sem fundo


---

## ESTRUTURA PADRÃO – PETIÇÃO BPC/LOAS

### Cabeçalho
O endereçamento ao juízo deve ser **um único parágrafo justificado** (`jc="both"`), em **negrito**, fonte Segoe UI 24 (12pt). O Word quebra a linha naturalmente pela justificação — nunca usar dois parágrafos separados para o cabeçalho.

```python
# CORRETO — um único parágrafo justificado
body += para(
    run("EXMO(A). SR(A). DR(A). JUIZ(A) FEDERAL DO JUIZADO ESPECIAL FEDERAL DE [CIDADE/UF]", bold=True),
    jc="both", fi=0, before=0, after=0
)
```

Após o endereçamento, inserir **5 linhas em branco** (5x `empty_line()`) antes da qualificação das partes.

### Partes — qualificação completa obrigatória

O parágrafo de qualificação das partes deve ser **um único bloco de texto contínuo** (um único `bp_r()`), nunca quebrado em múltiplos parágrafos. O Word fará a quebra de linha naturalmente ao final de cada linha — nunca inserir `empty()` ou novo parágrafo no meio da qualificação.

Formato obrigatório com **todos os campos**:

```
[NOME DO AUTOR em negrito], [nacionalidade], [estado civil], [profissão/ocupação],
portador do RG nº [RG] e CPF nº [CPF], nascido em [data], residente e domiciliado
à [endereço completo, número, bairro], CEP [CEP], [cidade/UF], representado neste ato por
sua [grau de parentesco] [NOME DO REPRESENTANTE em negrito], [nacionalidade],
[estado civil], [profissão], portadora do RG nº [RG] e CPF nº [CPF], residente
no mesmo endereço, vem, por intermédio de seu advogado, propor a presente
AÇÃO DE CONCESSÃO DE BENEFÍCIO DE PRESTAÇÃO CONTINUADA – BPC/LOAS [negrito]
em face do INSTITUTO NACIONAL DO SEGURO SOCIAL – INSS [negrito], Autarquia Federal,
CNPJ nº 29.979.036/0001-40, representado judicialmente pela Procuradoria Federal
Especializada junto ao INSS.
```

**REGRA OBRIGATÓRIA - CEP**: O CEP DEVE estar presente na qualificação das partes, sempre no formato `CEP XXXXX-XXX`. Nunca omitir o CEP. Se o CEP não for fornecido, solicitar ao usuário antes de gerar.

**Regras de formatação da qualificação:**
- Nome do autor: **negrito** em todas as ocorrências
- Nome do representante: **negrito** em todas as ocorrências
- **"AÇÃO DE CONCESSÃO DE BENEFÍCIO DE PRESTAÇÃO CONTINUADA – BPC/LOAS"**: **negrito**
- **"INSTITUTO NACIONAL DO SEGURO SOCIAL – INSS"**: **negrito** (seguido de texto normal: ", Autarquia Federal, CNPJ nº 29.979.036/0001-40, representado judicialmente pela Procuradoria Federal Especializada junto ao INSS.")
- Resto do texto: normal, preto
- Parágrafo único, justificado, recuo de primeira linha 0, `fi=0`
- Nunca cortar o parágrafo — deixar o Word fazer a quebra de linha naturalmente

Se algum dado não for fornecido pelo usuário, solicitar antes de gerar.

### Uso de nomes no corpo da petição
- Na qualificação das partes e na PRIMEIRA menção em "1 - DOS FATOS", usar o nome completo do autor e do representante em negrito.
- Nas menções SEGUINTES ao longo do texto, usar **"o autor"** ou **"a autora"** (conforme o gênero) em vez de repetir o nome completo. Para o representante, usar **"sua genitora"**, **"seu genitor"**, **"sua representante legal"** etc.
- NUNCA usar "requerente" — o termo correto é **"autor"** (masculino) ou **"autora"** (feminino).
- Isso evita repetição excessiva e torna o texto mais fluido e profissional.

### Seções obrigatórias
1. **1 - DOS FATOS**
   - 1.1 Do Requerimento Administrativo e do Indeferimento
   - 1.2 Da Condição de Saúde (descrição técnica da doença)
   - 1.3 Da Situação Socioeconômica (com tabela de gastos SOMENTE se houver valores informados)

2. **2 - DO DIREITO**
   - 2.1 Da Competência
   - 2.2 Do Direito ao BPC/LOAS
   - 2.3 Da Justiça Gratuita
   - 2.4 Da Miserabilidade e Comprometimento de Renda
   - 2.5 Da Tutela de Urgência
   - 2.6 Da Correção Monetária e dos Juros

### Seção 2.3 – Da Justiça Gratuita (modelo obrigatório)
Seção curta e objetiva (3 linhas no máximo):
> "A parte autora declara não possuir condições de arcar com as custas processuais e honorários advocatícios sem prejuízo do sustento próprio e de sua família, fazendo jus aos benefícios da justiça gratuita nos termos do art. 98 e seguintes do Código de Processo Civil."

### Regras sobre renda e gastos na seção 1.3
- Se TODOS os membros da família possuem renda R$ 0,00: mencionar o valor zero UMA vez e em seguida redigir que "a família não possui qualquer fonte de renda, encontrando-se em situação de extrema vulnerabilidade social e econômica".
- NÃO ficar repetindo "renda: R$ 0,00" para cada membro individualmente.
- **Tabela de gastos**: incluir na petição SOMENTE quando houver valores de gastos informados (valor > 0). Se os gastos não tiverem valores preenchidos, NÃO gerar a tabela — apenas mencionar no texto que a família possui gastos com saúde/tratamento.

3. **3 - DOS PEDIDOS** (letras a) a g) — os pedidos de honorários são numerados como 11 e 12 dentro da lista)
4. **4 - DO VALOR DA CAUSA**

---

## PEDIDOS – MODELO PADRÃO

Os pedidos de mérito são numerados sequencialmente (1, 2, 3...). Os itens de honorários vêm ao final, numerados como **11** e **12**, seguidos das letras f) e g) na estrutura de pedidos.

```
a) / 1.  Gratuidade da justiça (art. 98 CPC)
b) / 2.  Citação do INSS
c) / 3.  Tutela de urgência – implantação imediata + multa diária (arts. 300 e 537 CPC)
d) / 4.  Perícia médica e social com quesitos
e) / 5–10. Pedidos de mérito (procedência, retroativo, parcelas, correção...)
f) / 11. HONORÁRIOS DE SUCUMBÊNCIA
g) / 12. DESTAQUE DE HONORÁRIOS CONTRATUAIS
```

**IMPORTANTE**: O pedido a) de gratuidade da justiça é OBRIGATÓRIO e deve ser o primeiro pedido.

### Item f) – Honorários de sucumbência (redação obrigatória)
Texto normal em preto, mesmo estilo dos demais pedidos. Sem destaque visual de qualquer tipo.

Redação exata do item 11:
> "Condenar o réu ao pagamento de honorários advocatícios de sucumbência no patamar de 20% sobre o valor total da condenação, nos termos do artigo 85 do Código de Processo Civil."

### Item g) – Destaque de honorários contratuais (redação obrigatória)
Redação exata do item 12 (MANTER O TEXTO COMPLETO, apenas negritar o nome da sociedade):
> "Determinar, quando da expedição de RPV ou Precatório, o destaque dos honorários contratuais previstos no contrato juntado aos autos, com a expedição do respectivo pagamento em favor da sociedade **JOSÉ ROBERTO DA COSTA JUNIOR SOCIEDADE INDIVIDUAL DE ADVOCACIA – CNPJ nº 44.962.305/0001-50**, nos termos do art. 22, §4º da Lei nº 8.906/94."

**IMPORTANTE**: O texto completo do pedido deve ser mantido integralmente. Apenas o nome da sociedade e CNPJ ficam em negrito. O restante do texto permanece normal.
**Implementação**: Usar `bp_r()` com múltiplos `run()`:
```python
bp_r(
    run("12) ", bold=True) +
    run("Determinar, quando da expedicao de RPV ou Precatorio, o destaque dos honorarios contratuais previstos no contrato juntado aos autos, com a expedicao do respectivo pagamento em favor da sociedade ") +
    run("JOSE ROBERTO DA COSTA JUNIOR SOCIEDADE INDIVIDUAL DE ADVOCACIA - CNPJ no 44.962.305/0001-50", bold=True) +
    run(", nos termos do art. 22, par. 4o da Lei no 8.906/94."),
    fi=0, li=360
)
```
**Nome da sociedade e CNPJ são fixos** — nunca alterar.

---

## TABELA DE GASTOS FAMILIARES (embutida na petição)

Sempre incluir **dentro do corpo da petição** na seção 1.3 (Da Situação Socioeconômica), com os valores fornecidos pelo cliente.

Estrutura da tabela:
- Header: fundo cinza escuro `#333333`, texto branco
- Colunas: CATEGORIA | ITEM | VALOR MENSAL
- Linhas alternadas: cinza `#F2F2F2` e branco
- Linha de total: fundo `#E8E8E8`, texto preto, negrito
- **Não incluir coluna de "Observação" nem referência ao art. 20-B na tabela.** A fundamentação legal dos gastos com saúde como dedutíveis deve ser tratada no corpo do texto da petição, na argumentação jurídica, e não dentro da tabela de gastos.

---

## DESCRIÇÃO TÉCNICA DAS DOENÇAS

Para cada CID, incluir:
1. **Nomenclatura científica** da disfunção (ex: circuitos dopaminérgicos/noradrenérgicos)
2. **Critérios diagnósticos** (DSM-5 e/ou CID-11 conforme pertinente)
3. **Sintomas e impactos funcionais** concretos (familiar, escolar, clínico)
4. **Modelo biopsicossocial** (Lei 13.146/2015, art. 2º)
5. **Prognóstico**: tratamento contínuo, superior a 2 anos (art. 20, §2º, LOAS)
6. **Necessidades terapêuticas**: especialistas, medicação contínua

### TDAH – CID F90.0 (modelo validado)
- Disfunção dopaminérgica/noradrenérgica do córtex pré-frontal
- Funções executivas comprometidas: planejamento, controle inibitório, memória de trabalho, regulação da atenção
- DSM-5 (APA 2013) + CID-11 (OMS 2022)
- Impedimento de longo prazo sob modelo biopsicossocial (Lei 13.146/2015)

---

## PLANILHA DE CÁLCULO DE ATRASADOS (modelo Conta Fácil Prev - padrão tribunais federais)

Gerar **arquivo .xlsx separado** profissional e detalhado, no modelo aceito pelos tribunais federais (JF/RS - Conta Fácil Prev). A planilha deve ser **completa, organizada e autoexplicativa**, sem dados cortados ou ocultos.

### Estrutura obrigatória (3 abas)

#### ABA 1: "Resumo" (página principal)
Layout da aba Resumo (cada item = seção visual separada na planilha):

1. **Cabeçalho** (linhas 1-4, mescladas A:H):
   - Linha 1: "CONTA FÁCIL PREV" (negrito, tamanho 14, centralizado, fundo azul escuro #1F3864, texto branco)
   - Linha 2: "Programa para Cálculos em Ações Previdenciárias - INSS" (tamanho 11, centralizado, fundo azul escuro, texto branco)
   - Linha 3: vazia
   - Linha 4: "RESUMO DO CÁLCULO DO VALOR DA CAUSA" (negrito, tamanho 12, centralizado, fundo cinza claro #D9E2F3)

2. **Dados do processo** (linhas 6-14):
   - Layout em 2 colunas (A=rótulo cinza, B-D=valor):
     - Réu: INSTITUTO NACIONAL DO SEGURO SOCIAL - INSS
     - Autor(a): [nome completo]
     - Representante: [nome do representante] (se houver)
     - CPF: [CPF]
     - NB: [número do benefício]
     - DER: [data de entrada do requerimento]
     - Benefício: BPC/LOAS - Espécie 87
     - RMI: 1 salário mínimo (R$ [SM atual])
   - Rótulos: fundo #D9E2F3, negrito, alinhado à direita
   - Valores: sem fundo, alinhado à esquerda

3. **1 - PARTES** (tabela, a partir da linha 16):
   - Cabeçalho da tabela: fundo azul escuro #1F3864, texto branco, negrito
   - Colunas: A=Nome | B=Principal Corrigido | C=Juros Moratórios | D=Selic | E=Total (R$)
   - Linha de dados: nome do autor, valores calculados
   - **Linha Total Partes**: fundo #D9E2F3, negrito, com soma das colunas B:E
   - Valores monetários: formato "R$ #.##0,00" (number_format='#,##0.00')
   - Bordas finas em todas as células da tabela

4. **2 - TOTALIZAÇÃO** (linhas seguintes):
   - SUBTOTAL DA CONTA (1): valor (negrito)
   - Parcelas vincendas (12 × SM): valor
   - **TOTAL DA CONTA EM [MM/AAAA]**: fundo amarelo #FFF2CC, negrito, tamanho 12, borda dupla
   - Este é o **valor da causa** que vai na petição

5. **Assinatura** (4 linhas abaixo da totalização):
   - "Cálculo elaborado em [data]"
   - vazia
   - "Dr. José Roberto da Costa Junior"
   - "OAB/SP 378.163"

#### ABA 2: "Cálculo Detalhado" (parcelas mês a mês)
**Esta é a aba mais importante** - mostra cada parcela individualmente, mês a mês.

Layout:
1. **Cabeçalho** (linhas 1-2): Mesma formatação azul escuro da aba Resumo
   - "DEMONSTRATIVO DE CÁLCULO - PARCELAS VENCIDAS"
   - "Autor(a): [nome] | NB: [NB] | DER: [DER]"

2. **Tabela de parcelas vencidas** (a partir da linha 4):
   - Cabeçalho: fundo azul escuro #1F3864, texto branco, negrito
   - Colunas obrigatórias:
     - A: **Nº** (número sequencial da parcela: 1, 2, 3...)
     - B: **Competência** (mês/ano no formato MM/AAAA)
     - C: **Valor Original** (salário mínimo vigente no mês)
     - D: **Índice Correção** (fator INPC acumulado ou "Selic" se ≥01/2022)
     - E: **Principal Corrigido** (valor original × índice correção)
     - F: **Juros Moratórios** (0,00 - não apurados)
     - G: **Selic** (valor da Selic aplicada se ≥01/2022, senão 0,00)
     - H: **Total Parcela** (E + F + G)
   - **Cada mês em uma linha** - do mês seguinte à DER até o mês atual
   - Linhas alternadas: branco e cinza claro #F2F2F2 (zebrado)
   - Bordas finas em todas as células
   - **Linha SUBTOTAL VENCIDAS**: fundo #D9E2F3, negrito, soma das colunas C, E, F, G, H

3. **Tabela de parcelas vincendas** (abaixo das vencidas, separada por 2 linhas):
   - Título: "PARCELAS VINCENDAS (12 meses)" em negrito
   - Colunas: Nº | Competência | Valor (SM atual) | Total
   - 12 linhas (meses futuros a partir do mês atual)
   - **Linha SUBTOTAL VINCENDAS**: fundo #D9E2F3, negrito

4. **Totalização final**:
   - Total Vencidas: [soma]
   - Total Vincendas: [soma]
   - **TOTAL GERAL (VALOR DA CAUSA)**: fundo amarelo #FFF2CC, negrito, borda dupla

#### ABA 3: "Critérios"
Texto corrido com critérios e parâmetros do cálculo:
- Seção "CRITÉRIOS E PARÂMETROS DO CÁLCULO" (negrito, tamanho 12)
- Campos informativos (um por linha, rótulo negrito + valor):
  - Benefício: BPC/LOAS - Espécie 87
  - RMI: 1 salário mínimo
  - DER: [data]
  - DIP: mês seguinte à DER
  - Data base do cálculo: [data atual]
  - Moeda: Real (R$)
- Seção "CORREÇÃO MONETÁRIA":
  - "Índice utilizado: INPC (09/2006 em diante) até 12/2021"
  - "A partir de 01/2022: Taxa Selic como índice único (EC 113/2021)"
  - Composição histórica: ORTN (06/1964) → OTN (02/1986) → IPC/IBGE (01/1989) → BTN (05/1989) → IPC/IBGE (03/1990) → INPC (10/1991) → IRSM (01/1993) → URV (03/1994) → IPC-R (07/1994) → INPC (07/1995) → IGP-DI (04/1996) → INPC (01/2004) → Selic (01/2022)
- Seção "JUROS MORATÓRIOS": "Não foram apurados com data de início variável"
- Seção "HONORÁRIOS": "Não foram apurados" (honorários ficam APENAS na petição)
- Seção "PARCELAS VINCENDAS": "12 parcelas × salário mínimo vigente (sem correção)"
- Seção "FUNDAMENTAÇÃO LEGAL":
  - Art. 203, V, CF/88
  - Lei 8.742/1993 (LOAS), art. 20
  - EC 113/2021 (Taxa Selic)
  - Tema 810 STF (RE 870.947)
- Versão: Conta Fácil Prev 6.0.12

### Regras de formatação da planilha
- **Larguras de coluna**: ajustar automaticamente ao conteúdo (col A mais larga para nomes)
- **Congelar painéis**: congelar linha do cabeçalho da tabela em cada aba
- **Formato numérico**: usar `number_format = '#,##0.00'` para valores monetários
- **Bordas**: `Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))` em todas as células de tabela
- **Impressão**: configurar `sheet.print_area`, `sheet.page_setup.orientation = 'landscape'`, margens estreitas
- **Fonte padrão**: Calibri 10 para dados, Calibri 11 negrito para cabeçalhos

### Cálculo dos valores
- **Parcelas vencidas**: mês seguinte à DER até mês atual. Cada mês = SM vigente naquele mês
  - SM 2024: R$ 1.412,00 | SM 2025: R$ 1.518,00 | SM 2026: R$ 1.621,00
  - Correção INPC: aplicar fator acumulado estimado (usar 0,5% a.m. como estimativa quando índice real não disponível)
  - Selic: aplicar sobre parcelas de 01/2022 em diante (usar 1% a.m. como estimativa)
- **Parcelas vincendas**: 12 × SM vigente (R$ 1.621,00 em 2026), sem correção
- **Juros moratórios**: R$ 0,00 (não apurados - padrão Conta Fácil Prev)
- **Total geral = valor da causa**: soma de vencidas corrigidas + vincendas

---

## QUESITOS PERICIAIS (documentos separados)

Gerar **dois documentos .docx separados**, cada um com timbrado:

### Qualificação nos quesitos
Os quesitos devem conter apenas uma **qualificação resumida** do autor no início, antes das seções de perguntas:
> "Autor(a): [NOME COMPLETO], [idade] anos, CPF nº [CPF], representado por [NOME DO REPRESENTANTE] ([parentesco]). NB nº [NB]. CID-10: [CIDs]."

NÃO incluir qualificação completa com endereço, RG, nacionalidade etc. nos quesitos — isso já consta na petição.

### Quesitos Perícia Médica – estrutura
- Cabeçalho: "QUESITOS PARA PERÍCIA MÉDICA"
- Subtítulo: "Ação BPC/LOAS | NB nº [X] | CID-10: [X]"
- Seções:
  1. **1 - Sobre o Diagnóstico** (~5 quesitos): confirmação do CID, critérios DSM/CID-11, caráter permanente, data início, comorbidades
  2. **2 - Sobre o Impedimento Funcional** (~6 quesitos): modelo biopsicossocial Lei 13.146/2015, grau de comprometimento, necessidades terapêuticas, medicação, Protocolo de Avaliação Biopsicossocial
  3. **3 - Sobre o Prognóstico** (~4 quesitos): previsão de remissão, capacidade adaptativa, suficiência do SUS, prazo >2 anos
  4. **4 - Quesitos Complementares** (~2 quesitos): divergências com laudo, esclarecimentos adicionais

### Quesitos Perícia Social – estrutura
- Cabeçalho: "QUESITOS PARA PERÍCIA SOCIAL"
- Subtítulo: "Ação BPC/LOAS | NB nº [X] | Grupo Familiar: [X] membros"
- Seções:
  1. **1 - Composição Familiar e Renda** (~6 quesitos): membros, renda bruta, Bolsa Família, renda per capita, patrimônio
  2. **2 - Condições de Moradia e Vulnerabilidade** (~4 quesitos): situação habitacional, saneamento, vulnerabilidades, escola
  3. **3 - Gastos com o Autor** (~4 quesitos): verificação in loco dos gastos declarados, outros gastos não declarados, comprometimento de renda, capacidade laborativa do responsável
  4. **4 - Rede de Suporte e Serviços Públicos** (~4 quesitos): suporte familiar, serviços SUS disponíveis, impacto do indeferimento, esclarecimentos adicionais

### Formatação dos quesitos
- Numeração: `1. `, `2. ` (sem bullets, sem traços)
- Texto em preto, corrido, justificado
- Recuo de 360 twips
- Espaçamento after 160 (para respirar entre quesitos)

---

## ASSINATURA PADRÃO (todos os documentos)

```
[cidade], [data por extenso].

Dr. José Roberto da Costa Junior
OAB/SP 378.163
```
- **"Dr. José Roberto da Costa Junior"**: negrito, centralizado, preto
- **"OAB/SP 378.163"**: negrito, tamanho 22 (11pt), preto, centralizado
- Data: alinhada à direita

---

## MÉTODO TÉCNICO – CONSTRUÇÃO DO DOCX

### Base obrigatória
```python
# 1. Copiar timbrado como base
shutil.copy('timbrado.docx', '/tmp/base.docx')
with zipfile.ZipFile('/tmp/base.docx','r') as z:
    z.extractall('/tmp/base_dir')

# 2. Extrair sectPr do documento original para preservar margens/header
with open('/tmp/base_dir/word/document.xml', encoding='utf-8') as f:
    orig = f.read()
sect = re.search(r'<w:sectPr[^>]*>.*?</w:sectPr>', orig, re.DOTALL)
sect_pr = sect.group()  # SEMPRE incluir no final do body

# 3. Namespaces mínimos necessários
ns = ('xmlns:r="..." xmlns:w="..." xmlns:w14="..." xmlns:w15="..." '
      'xmlns:mc="..." mc:Ignorable="w14 w15"')

# 4. Montar documento
new_doc = f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<w:document {ns}><w:body>{body_xml}{sect_pr}</w:body></w:document>'
```

### Funções helper padrão
```python
def esc(t): return str(t).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')

def run(text, bold=False, sz="24", caps=False, italic=False):
    """Run com Segoe UI – tudo em preto, sem parâmetro de cor"""
    rpr = '<w:rFonts w:ascii="Segoe UI" w:hAnsi="Segoe UI" w:cs="Segoe UI"/>'
    if bold:   rpr += '<w:b/><w:bCs/>'
    if italic: rpr += '<w:i/><w:iCs/>'
    if caps:   rpr += '<w:caps/>'
    rpr += f'<w:sz w:val="{sz}"/><w:szCs w:val="{sz}"/>'
    return f'<w:r><w:rPr>{rpr}</w:rPr><w:t xml:space="preserve">{esc(text)}</w:t></w:r>'

def para(runs_str, jc="both", fi=720, li=0, before=0, after=100, line=360,
         shd=None, bdr_top=False, bdr_bot=False):
    ppr  = f'<w:spacing w:before="{before}" w:after="{after}" w:line="{line}" w:lineRule="auto"/>'
    ppr += f'<w:jc w:val="{jc}"/><w:ind w:firstLine="{fi}" w:left="{li}"/>'
    if shd: ppr += f'<w:shd w:val="clear" w:color="auto" w:fill="{shd}"/>'
    if bdr_top or bdr_bot:
        ppr += '<w:pBdr>'
        if bdr_top: ppr += '<w:top w:val="single" w:sz="12" w:space="4" w:color="000000"/>'
        if bdr_bot: ppr += '<w:bottom w:val="single" w:sz="12" w:space="4" w:color="000000"/>'
        ppr += '</w:pBdr>'
    return f'<w:p><w:pPr>{ppr}</w:pPr>{runs_str}</w:p>'

def sec_title(t):
    return para(run(t, bold=True, sz="22", caps=True),
                jc="center", fi=0, before=160, after=160, shd="F2F2F2",
                bdr_top=True, bdr_bot=True)

def sub_title(t):
    return para(run(t, bold=True, sz="22"),
                jc="left", fi=0, before=160, after=80)

def bp(text, fi=720):
    """Parágrafo body simples"""
    return para(run(text), jc="both", fi=fi, before=0, after=100)

def bp_r(runs_str, fi=720, li=0):
    """Parágrafo body com múltiplos runs (para nomes em negrito no meio do texto)"""
    return para(runs_str, jc="both", fi=fi, li=li, before=0, after=100)

def ped(letra, texto):
    """Item de pedido: a) texto..."""
    return para(run(f"{letra}) ", bold=True) + run(texto),
                jc="both", fi=0, li=360, before=0, after=120)

def quesito(num, texto):
    """Item de quesito: 1. texto..."""
    return para(run(f"{num}. ", bold=True) + run(texto),
                jc="both", fi=0, li=360, before=0, after=160)

def empty():
    return '<w:p><w:pPr><w:spacing w:before="80" w:after="0"/></w:pPr></w:p>'
```

---

## DADOS FIXOS DO ESCRITÓRIO

| Campo | Valor |
|---|---|
| Advogado | Dr. José Roberto da Costa Junior |
| OAB | OAB/SP 378.163 |
| Timbrado | `timbrado.docx` |
| Fonte | Segoe UI |
| Cor do documento | Preto e branco apenas |
| SM 2024 | R$ 1.412,00 |
| SM 2025 | R$ 1.518,00 |
| SM 2026 | R$ 1.621,00 |
| JEF competência | < 60 salários mínimos |

---

## LEGISLAÇÃO E JURISPRUDÊNCIA VERIFICADAS

Todas as referências legais utilizadas nesta skill foram verificadas quanto à existência e aplicação correta:

| Referência | Status | Observação |
|---|---|---|
| Art. 203, V, CF/88 | Valido | Garante 1 SM a pessoa com deficiência/idoso sem meios de subsistência |
| Art. 20, Lei 8.742/1993 (LOAS) | Valido | Regulamenta o BPC, define requisitos |
| Art. 20, par. 2, LOAS | Valido | Define pessoa com deficiência para fins de BPC (impedimento longo prazo) |
| Art. 20, par. 3, LOAS | Valido | Critério de renda per capita de 1/4 SM |
| Art. 20-B, Lei 8.742/1993 | Valido | Incluído pela Lei 14.176/2021, permite dedução de gastos com saúde da renda |
| Art. 98, CPC | Valido | Gratuidade da justiça |
| Arts. 300 e 537, CPC | Validos | Tutela de urgência e multa por descumprimento |
| Art. 85, CPC | Valido | Honorários advocatícios |
| Art. 85, par. 14, CPC | Valido | Execução autônoma de honorários pelo advogado |
| Súmula 111, STJ | Valida | Honorários incidem sobre parcelas vencidas até a sentença |
| Súmula 204, STJ | Valida | Juros de mora desde a citação válida em benefícios previdenciários |
| Lei 13.146/2015, art. 2 | Valido | Estatuto da Pessoa com Deficiência, modelo biopsicossocial |
| Lei 12.764/2012 | Valida | Lei Berenice Piana: TEA equiparado a deficiência |
| Lei 14.176/2021 | Valida | Alterou a LOAS, criou art. 20-B |
| Tema 810 STF (RE 870.947) | Valido | Correção monetária e juros contra a Fazenda Pública |
| EC 113/2021 | Valida | Taxa Selic como índice único a partir de 09/12/2021 |
| Rcl 4.374/PE e RE 567.985/MT | Validos | Inconstitucionalidade parcial do critério de 1/4 SM |
| Lei 10.259/2001, art. 3 | Valido | Competência do JEF até 60 SM |
| DSM-5 (APA, 2013) | Valido | Manual diagnóstico de transtornos mentais |
| CID-11 (OMS, 2022) | Valido | Classificação internacional de doenças, versão atual |

Ao redigir petições, citar apenas legislação e jurisprudência que comprovadamente existam. Caso o modelo não tenha certeza sobre alguma referência, omiti-la em vez de arriscar uma citação incorreta.

---

## CHECKLIST ANTES DE GERAR

- [ ] Timbrado copiado como base do docx
- [ ] sectPr do original preservado (margens + header com imagem de fundo)
- [ ] Fonte Segoe UI em todos os runs
- [ ] Documento inteiro em preto e branco (sem cores azuis ou de qualquer tipo)
- [ ] Nomes dos clientes em **negrito** em TODAS as ocorrências
- [ ] **Dr. José Roberto da Costa Junior** e **OAB/SP 378.163** em negrito
- [ ] Numeração com números arábicos (1, 2, 3) e não romanos (I, II, III)
- [ ] Cabeçalho sem sub-header com artigos/espécies (apenas endereçamento ao juízo)
- [ ] Seção 1.1 = Requerimento e Indeferimento (vem antes de saúde e socioeconômico)
- [ ] Tabela de gastos sem coluna de observação e sem menção ao art. 20-B
- [ ] Zero traços decorativos no texto
- [ ] CEP presente na qualificação das partes (formato CEP XXXXX-XXX)
- [ ] Cabeçalho com 5 linhas em branco antes das partes
- [ ] Honorários item f) = item 11: sucumbência 20% art. 85 CPC
- [ ] Honorários item g) = item 12: destaque honorários contratuais com nome e CNPJ da sociedade corretos
- [ ] Toda legislação e jurisprudência citada é real e verificada
- [ ] Planilha de cálculo com 3 abas (Resumo, Cálculo Detalhado, Critérios)
- [ ] Planilha com parcelas mês a mês na aba "Cálculo Detalhado"
- [ ] Planilha com totais e subtotais destacados (fundo amarelo/azul)
- [ ] Planilha com formatação profissional (bordas, zebrado, cabeçalhos coloridos)
- [ ] Planilha com honorários = "Não foram apurados"
- [ ] Quesitos em documentos separados com timbrado
- [ ] Assinatura: nome e OAB em negrito, preto, centralizados
- [ ] Data por extenso, alinhada à direita

---

## OUTPUTS PADRÃO PARA CASO BPC/LOAS COMPLETO

1. `peticao_bpc_[cliente].docx` — petição inicial completa
2. `calculo_atrasados_[cliente].xlsx` — planilha modelo Conta Fácil Prev
3. `quesitos_pericia_medica_[cliente].docx` — quesitos médicos
4. `quesitos_pericia_social_[cliente].docx` — quesitos sociais

---

## INSTRUÇÃO ESPECIAL PARA API

Quando receber os dados do cliente, você DEVE:

1. Gerar um **único bloco de código Python** que crie o documento solicitado
2. O código deve usar as funções helper acima e o método técnico de construção do DOCX
3. Os arquivos devem ser salvos no diretório `output/` (usar variável OUTPUT_DIR)
4. Para a planilha .xlsx, usar `openpyxl`
5. O código deve ser **autocontido** e executável diretamente
6. Use `import shutil, zipfile, re, os` no início (e `import openpyxl` para planilhas)
7. O TIMBRADO_PATH é `'timbrado.docx'` e OUTPUT_DIR é `'output'`
8. Retornar o código dentro de um bloco ```python ... ```
9. NÃO usar emojis ou caracteres unicode especiais no código
10. NÃO usar caminhos /mnt/ ou /tmp/ — usar OUTPUT_DIR para tudo
