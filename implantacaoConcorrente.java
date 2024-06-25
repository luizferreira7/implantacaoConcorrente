package br.ufrj.siga.preMatriculaApi.service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ImportacaoCandidatoService {

    private static final int BATCH_SIZE = 100;

    private final EntityManager entityManager;

    private final Batcher batcher;

    private final ConcurrentProperties concurrentProperties;

    public void implantar(Chamada chamadaAbrir) throws InterruptedException, ExecutionException
    {
        PeriodoConcurso periodoConcurso = periodoConcursoService.recuperarPeriodoConcursoEmAndamento();

        List<ImportacaoCandidato> importacaoCandidatos = Collections.synchronizedList(importacaoCandidatoRepository.recuperarImportacaoCandidatosPorChamadaOid(chamadaAbrir.getOid()));

        TipoEndereco tipoEndereco = tipoEnderecoRepository.findTipoEnderecoByDescricao(TipoEndereco.DESCRICAO_RESIDENCIAL);
        TipoTelefone tipoTelefone = tipoTelefoneRepository.findTelefoneByDescricao(TipoTelefone.DESCRICAO_RESIDENCIAL);
        Pais paisBrasil = paisRepository.getBrasil();
        Situacao situacaoB01 = situacaoService.recuperarSituacaoPorCodigo(Situacao.B01);
        Usuario usuarioSistema = usuarioService.getUsuarioSistema();

        Map<Integer, Curso> mapaCursos = Collections.synchronizedMap(cursoService.recuperarMapaCurso(chamadaAbrir.getConcurso().getOid()));
        Map<Integer, Fila> mapaFilas = Collections.synchronizedMap(filaService.recuperarMapaFilas());

        List<List<ImportacaoCandidato>> lotesImportacaoCandidato = Collections.synchronizedList(ListUtils.partition(importacaoCandidatos, BATCH_SIZE));

        log.info("Total de lotes: {}", lotesImportacaoCandidato.size());

        Instant start = Instant.now();

        DadosExistentesDTO dadosExistentes = preProcessamentoDadosExistentes(lotesImportacaoCandidato, periodoConcurso);

        Instant finish = Instant.now();

        long timeElapsedSeconds = Duration.between(start, finish).toSeconds();

        log.info("PreProcessamento finalizado em {} segundos", timeElapsedSeconds);

        ThreadPoolExecutor executorPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(concurrentProperties.getThreadNumber());
        List<CompletableFuture<ThreadDataImportacaoCandidato>> futures = new ArrayList<>();

        start = Instant.now();

        log.info("Processamento iniciado");

        int contador = 1;
        for (List<ImportacaoCandidato> loteImportacaoCandidato : lotesImportacaoCandidato) {
            int finalContador = contador;

            CompletableFuture<ThreadDataImportacaoCandidato> future = CompletableFuture.supplyAsync(() -> {
                //log.info("Processando Lote: {} - Tamanho: {}", finalContador, loteImportacaoCandidato.size());

                return processarLoteImportacaoCandidato(loteImportacaoCandidato, chamadaAbrir, periodoConcurso,
                    finalContador, tipoTelefone, tipoEndereco, paisBrasil, mapaCursos, mapaFilas,
                    situacaoB01, usuarioSistema, dadosExistentes);
            }, executorPool);

            futures.add(future);
            contador++;
        }

        List<ThreadDataImportacaoCandidato> threadDatasImportacaoCandidato = futures.stream().map(CompletableFuture::join).toList();

        finish = Instant.now();

        timeElapsedSeconds = Duration.between(start, finish).toMillis();

        log.info("Processamento finalizado em {} milis", timeElapsedSeconds);

        start = Instant.now();

        log.info("Persistencia iniciada");

        CountDownLatch countDownLatch = new CountDownLatch(threadDatasImportacaoCandidato.size());

        for (ThreadDataImportacaoCandidato threadDataImportacaoCandidato : threadDatasImportacaoCandidato)
        {
            executorPool.submit(() -> {
                persistirLote(threadDataImportacaoCandidato);
                countDownLatch.countDown();
            });
        }

        countDownLatch.await();

        finish = Instant.now();

        timeElapsedSeconds = Duration.between(start, finish).toMillis();

        log.info("Persistencia finalizada em {} milis", timeElapsedSeconds);

        executorPool.shutdownNow();

        log.info("Implantação concluida");
    }

    private void persistirLote(ThreadDataImportacaoCandidato threadDataImportacaoCandidato)
    {
        //log.info("Persistindo lote: {}", threadDataImportacaoCandidato.getLote());

        batcher.persistirLote(threadDataImportacaoCandidato.getDadosPessoais());
        batcher.persistirLote(threadDataImportacaoCandidato.getCandidatos());
        batcher.persistirLote(threadDataImportacaoCandidato.getDadosEnem());
        batcher.persistirLote(threadDataImportacaoCandidato.getOpcoesCurso());
        batcher.persistirLote(threadDataImportacaoCandidato.getHistoricosOpcao());
        batcher.persistirLote(threadDataImportacaoCandidato.getPerfisSocioEconomico());

        batcher.persistirLote(threadDataImportacaoCandidato.getTelefones());
        batcher.persistirLote(threadDataImportacaoCandidato.getEnderecos());

        batcher.persistirLote(threadDataImportacaoCandidato.getDadosEnemOpcaoCurso());

        //log.info("Lote {} persistido", threadDataImportacaoCandidato.getLote());
    }

    private DadosExistentesDTO preProcessamentoDadosExistentes(List<List<ImportacaoCandidato>> lotesImportacaoCandidatos, PeriodoConcurso periodoConcurso) throws InterruptedException
    {
        log.info("Preprocessamento iniciado");

        DadosExistentesDTO dadosExistentesDTO = new DadosExistentesDTO();

        ThreadPoolExecutor executorPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(concurrentProperties.getThreadNumber());
        CountDownLatch countDownLatch = new CountDownLatch(lotesImportacaoCandidatos.size());

        Semaphore semaphore = new Semaphore(1);

        int contador = 1;
        for (List<ImportacaoCandidato> lote : lotesImportacaoCandidatos)
        {
            int finalContador = contador;
            executorPool.submit(() -> {
                //log.info("Preprocessamento lote: {}", finalContador);

                Map<String, String> mapaDadosPessoaisExistentes = Collections.synchronizedMap(dadosPessoaisService.montarMapaCpfDadosPessoais(lote.stream().map(ImportacaoCandidato::getCpf).collect(Collectors.toSet())));
                Map<String, String> mapaDadosEnemExistentes =  Collections.synchronizedMap(dadosEnemService.montarMapaNumeroEnemDadosEnemOid(lote.stream().map(ImportacaoCandidato::getNumeroEnem).collect(Collectors.toSet())));
                Map<String, String> mapaCandidatosExistentes = Collections.synchronizedMap(candidatoService.montarMapaCpfCandidato(mapaDadosPessoaisExistentes.values().stream().toList(), periodoConcurso));
                Map<String, String> mapaCep = Collections.synchronizedMap(cepService.montarMapaCepPorCeps(lote.stream().map(ImportacaoCandidato::getCep).toList()));

                try
                {
                    semaphore.acquire();

                    dadosExistentesDTO.getMapaDadosEnemExistentes().putAll(mapaDadosEnemExistentes);
                    dadosExistentesDTO.getMapaDadosPessoaisExistentes().putAll(mapaDadosPessoaisExistentes);
                    dadosExistentesDTO.getMapaCep().putAll(mapaCep);
                    dadosExistentesDTO.getMapaCandidatosExistentes().putAll(mapaCandidatosExistentes);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
                finally
                {
                    //log.info("Preprocessamento Lote: {} concluido", finalContador);
                    semaphore.release();
                }

                countDownLatch.countDown();
            });

            contador++;
        }

        countDownLatch.await();

        executorPool.shutdownNow();

        return dadosExistentesDTO;
    }

    public ThreadDataImportacaoCandidato processarLoteImportacaoCandidato(List<ImportacaoCandidato> importacaoCandidatos,
                                                                          Chamada chamada,
                                                                          PeriodoConcurso periodoConcurso,
                                                                          Integer lote,
                                                                          TipoTelefone tipoTelefone,
                                                                          TipoEndereco tipoEndereco,
                                                                          Pais pais,
                                                                          Map<Integer, Curso> mapaCursos,
                                                                          Map<Integer, Fila> mapaFila,
                                                                          Situacao situacaoB01,
                                                                          Usuario usuarioSistema,
                                                                          DadosExistentesDTO dadosExistentesDTO)
    {
        ThreadDataImportacaoCandidato threadDataImportacaoCandidato = new ThreadDataImportacaoCandidato(lote);

        for (ImportacaoCandidato importacaoCandidato : importacaoCandidatos)
        {
            DadosPessoais dadosPessoais = processarDadosPessoais(threadDataImportacaoCandidato, importacaoCandidato,
                tipoEndereco, pais, dadosExistentesDTO.getMapaCep(), tipoTelefone, dadosExistentesDTO.getMapaDadosPessoaisExistentes());

            Candidato candidato = processaCandidato(threadDataImportacaoCandidato, importacaoCandidato,
                dadosPessoais, periodoConcurso, dadosExistentesDTO.getMapaCandidatosExistentes());

            OpcaoCurso opcaoCurso = processaOpcaoCurso(threadDataImportacaoCandidato, importacaoCandidato, candidato, chamada,
                situacaoB01, mapaCursos, mapaFila, usuarioSistema);

            processarDadosEnem(threadDataImportacaoCandidato, importacaoCandidato, opcaoCurso,
                dadosExistentesDTO.getMapaDadosEnemExistentes());
        }

        //log.info("Lote {} Concluido", lote);

        return threadDataImportacaoCandidato;
    }

    public void processarDadosEnem(ThreadDataImportacaoCandidato threadDataImportacaoCandidato,
                                   ImportacaoCandidato importacaoCandidato,
                                   OpcaoCurso opcaoCurso,
                                   Map<String, String> mapaDadosEnemExistentes)
    {
        DadosEnem dadosEnem;

        String dadosEnemOid = mapaDadosEnemExistentes.get(importacaoCandidato.getNumeroEnem());

        if (dadosEnemOid != null) {
            dadosEnem = entityManager.find(DadosEnem.class, dadosEnemOid);
        }
        else
        {
            dadosEnem = dadosEnemService.criarDadosEnem(importacaoCandidato);

            threadDataImportacaoCandidato.getDadosEnem().add(dadosEnem);
        }

        DadosEnemOpcaoCurso dadosEnemOpcaoCurso = dadosEnemService.criarDadosEnemOpcaoCurso(dadosEnem, opcaoCurso);

        threadDataImportacaoCandidato.getDadosEnemOpcaoCurso().add(dadosEnemOpcaoCurso);
    }

    private DadosPessoais processarDadosPessoais(ThreadDataImportacaoCandidato threadDataImportacaoCandidato,
                                                 ImportacaoCandidato importacaoCandidato,
                                                 TipoEndereco tipoEndereco,
                                                 Pais pais,
                                                 Map<String, String> mapaCep,
                                                 TipoTelefone tipoTelefone,
                                                 Map<String, String> mapaDadosPessoais)
    {
        DadosPessoais dadosPessoais;

        String dadosPessoaisOid = mapaDadosPessoais.get(importacaoCandidato.getCpf());

        if (dadosPessoaisOid != null)
        {
            dadosPessoais = entityManager.find(DadosPessoais.class, dadosPessoaisOid);
        }
        else
        {
            dadosPessoais = dadosPessoaisService.criaDadosPessoais(importacaoCandidato);
            threadDataImportacaoCandidato.getDadosPessoais().add(dadosPessoais);
        }

        String cepOid = mapaCep.get(importacaoCandidato.getCep());

        if (cepOid != null) {
            CEP cep = entityManager.find(CEP.class, cepOid);

            Endereco endereco = enderecoService.criaEndereco(importacaoCandidato, tipoEndereco, pais, cep, dadosPessoais);

            threadDataImportacaoCandidato.getEnderecos().add(endereco);
        }

        if (StringUtils.isNotBlank(importacaoCandidato.getTelefone1())) {
            Telefone telefone1 = telefoneService.criaTelefone(importacaoCandidato.getTelefone1(), tipoTelefone, dadosPessoais);
            threadDataImportacaoCandidato.getTelefones().add(telefone1);
        }

        if (StringUtils.isNotBlank(importacaoCandidato.getTelefone2())) {
            Telefone telefone2 = telefoneService.criaTelefone(importacaoCandidato.getTelefone2(), tipoTelefone, dadosPessoais);
            threadDataImportacaoCandidato.getTelefones().add(telefone2);
        }

        return dadosPessoais;
    }

    private Candidato processaCandidato(ThreadDataImportacaoCandidato threadDataImportacaoCandidato,
                                        ImportacaoCandidato importacaoCandidato,
                                        DadosPessoais dadosPessoais,
                                        PeriodoConcurso periodoConcurso,
                                        Map<String, String> mapaCandidatos)
    {
        Candidato candidato;

        String candidatoOid = mapaCandidatos.get(importacaoCandidato.getCpf());

        if (candidatoOid != null) {
            candidato = entityManager.find(Candidato.class, candidatoOid);
        }
        else
        {
            candidato = candidatoService.criaCandidato(importacaoCandidato, periodoConcurso, dadosPessoais);
            threadDataImportacaoCandidato.getCandidatos().add(candidato);
        }

        return candidato;
    }

    private OpcaoCurso processaOpcaoCurso(ThreadDataImportacaoCandidato threadDataImportacaoCandidato,
                                          ImportacaoCandidato importacaoCandidatos,
                                          Candidato candidato, Chamada chamada,
                                          Situacao situacaoB01,
                                          Map<Integer, Curso> mapaCursos,
                                          Map<Integer, Fila> mapaFilas,
                                          Usuario usuarioSistema)
    {

        OpcaoCurso opcaoCurso = opcaoCursoService.criaOpcaoCurso(importacaoCandidatos, candidato, chamada, situacaoB01, mapaCursos, mapaFilas);
        HistoricoOpcao historicoOpcao = historicoOpcaoService.processaHistoricoOpcao(usuarioSistema, opcaoCurso);
        PerfilSocioEconomico perfilSocioEconomico = perfilSocioEconomicoService.criarPerfilSocioEconomico(importacaoCandidatos, opcaoCurso);

        threadDataImportacaoCandidato.getOpcoesCurso().add(opcaoCurso);
        threadDataImportacaoCandidato.getHistoricosOpcao().add(historicoOpcao);
        threadDataImportacaoCandidato.getPerfisSocioEconomico().add(perfilSocioEconomico);

        return opcaoCurso;
    }
}
