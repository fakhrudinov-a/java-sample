
@Slf4j
@RequiredArgsConstructor
public abstract class ExportTaskDomainService<T, F extends ExportTaskFilter, K extends Comparable<K>> {

    protected final EntityManager entityManager;
    protected final CommonExportTaskService commonExportTaskService;
    protected final S3MultipartUploadService s3MultipartUploadService;

    public void tryToExecute(ExportTaskDto<F> exportTaskDto) {
        try {
            commonExportTaskService.markExportTaskInProgress(exportTaskDto.getFilter().getRequestId());
            execute(exportTaskDto);
        } catch (Exception e) {
            log.error("Couldn't execute report task for domain {} with request id {}. Exception message: [{}]",
                    exportTaskDto.getDomain(), exportTaskDto.getFilter().getRequestId(), e.getMessage(), e);
            commonExportTaskService.failExportTask(exportTaskDto.getFilter().getRequestId());
        }
    }

    public void saveInS3AndUpdateTask(
            ExportTaskS3Request<T> request,
            ExportTask exportTask
    ) throws IOException, UploadFailedException {
        var s3Path = saveInS3AndGetKey(request);

        exportTask.setS3Path(s3Path);
        exportTask.setStatus(ExportTaskStatus.READY);
        commonExportTaskService.update(exportTask);
    }

    public abstract ExportDomain getDomainCode();

    protected abstract Integer getMaxDataBatchSize();

    protected abstract K getLastSeenId(T entity);

    protected abstract Class<T> getEntityClass();

    protected abstract String getIdColumnName();

    protected abstract void execute(ExportTaskDto<F> exportTaskDto) throws UploadFailedException, IOException;

    protected List<T> loadDataWithKeySet(Specification<T> specification, K lastSeenId) {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<T> query = criteriaBuilder.createQuery(getEntityClass());
        Root<T> root = query.from(getEntityClass());

        Predicate predicate = specification.toPredicate(root, query, criteriaBuilder);

        if (lastSeenId != null) {
            predicate = criteriaBuilder.and(predicate,
                    criteriaBuilder.greaterThan(root.get(getIdColumnName()), lastSeenId));
        }

        Order order = criteriaBuilder.asc(root.get(getIdColumnName()));
        query.orderBy(order);
        query.where(predicate);

        TypedQuery<T> typedQuery = entityManager.createQuery(query);
        typedQuery.setMaxResults(getMaxDataBatchSize());

        getFetchGraph().ifPresent(graph -> {
                    var entityGraph = entityManager.getEntityGraph(graph);
                    typedQuery.setHint("jakarta.persistence.fetchgraph", entityGraph);
                }
        );

        return typedQuery.getResultList();
    }

    protected Optional<String> getFetchGraph() {
        return Optional.empty();
    }

    private String saveInS3AndGetKey(
            ExportTaskS3Request<T> request
    ) throws IOException, UploadFailedException {
        String key = constructS3Key(request);
        ExportFileFormat exportFileFormat = request.format();

        CreateMultipartUploadResponse multipartUploadResponse = s3MultipartUploadService
                .createMultipartUploadResponse(key, exportFileFormat.getContentType(), S3Bucket.EXPORT);
        String uploadId = multipartUploadResponse.uploadId();

        int currentPartNumber = 1;
        int currentPartSize = 0;
        int currentPage = 0;
        K lastSeenId = null;
        boolean lastBatch = false;
        List<CompletedPart> completedParts = new ArrayList<>();
        List<T> accumulatedRecords = new ArrayList<>();

        while (!lastBatch) {
            List<T> recordsList = loadDataWithKeySet(request.specification(), lastSeenId);

            if (CollectionUtils.isEmpty(recordsList) && currentPage == 0) {
                uploadOnlyHeaders(uploadId, key, exportFileFormat, completedParts, request.columnMappings());
                break;
            }
            if (isEmptyPage(accumulatedRecords, recordsList)) {
                break;
            }

            accumulatedRecords.addAll(recordsList);
            currentPartSize += processRecordsAndGetBytesSize(recordsList, exportFileFormat, request.columnMappings());

            lastBatch = isLastBatch(recordsList.size());

            if (s3MultipartUploadService.shouldUploadPart(lastBatch, currentPartSize)) {
                byte[] accumulatedRecordsInBytes = preparePartToUpload(
                        accumulatedRecords,
                        exportFileFormat,
                        currentPartNumber,
                        request.columnMappings());

                s3MultipartUploadService.uploadPart(
                        uploadId, key, accumulatedRecordsInBytes, currentPartNumber, completedParts, S3Bucket.EXPORT);

                accumulatedRecords.clear();
                currentPartSize = 0;
                currentPartNumber++;
            }

            if (!lastBatch) {
                lastSeenId = getLastSeenId(recordsList.getLast());
            }
            currentPage++;
        }

        s3MultipartUploadService.complete(completedParts, key, multipartUploadResponse.uploadId(), S3Bucket.EXPORT);

        return key;
    }

    private boolean isEmptyPage(List<T> accumulatedRecords, List<T> recordsList) {
        return CollectionUtils.isEmpty(recordsList) && CollectionUtils.isEmpty(accumulatedRecords);
    }

    private boolean isLastBatch(int recordsSize) {
        return getMaxDataBatchSize() > recordsSize;
    }

    private void uploadOnlyHeaders(
            String uploadId,
            String key,
            ExportFileFormat format,
            List<CompletedPart> completedParts,
            Map<String, Function<T, Object>> columnMappings
    ) throws UploadFailedException {
        byte[] header = ExportTaskConverter.convertHeadersToFormatInBytes(format, columnMappings);
        s3MultipartUploadService.uploadPart(uploadId, key, header, 1, completedParts, S3Bucket.EXPORT);
    }

    private String constructS3Key(ExportTaskS3Request<T> request) {
        return S3Utils.constructObjectKey(
                EXPORT_TASK_FOLDER_FORMAT.formatted(
                        request.domainLabel(), request.userId(),
                        request.exportTaskId()), request.fileName()
        );
    }

    private int processRecordsAndGetBytesSize(
            List<T> recordsList,
            ExportFileFormat format,
            Map<String, Function<T, Object>> columnMappings
    ) {
        byte[] dataInBytes = ExportTaskConverter.convertToFormatReturningBytes(recordsList, columnMappings, format);
        return dataInBytes.length;
    }

    private byte[] preparePartToUpload(
            List<T> accumulatedRecords,
            ExportFileFormat format,
            int currentPartNumber,
            Map<String, Function<T, Object>> columnMappings
    ) throws IOException {
        byte[] accumulatedRecordsInBytes = ExportTaskConverter.convertToFormatReturningBytes(accumulatedRecords,
                columnMappings, format);

        if (currentPartNumber != 1) {
            return accumulatedRecordsInBytes;
        }
        byte[] header = ExportTaskConverter.convertHeadersToFormatInBytes(format, columnMappings);
        return ExportTaskConverter.concatenateByteArrays(header, accumulatedRecordsInBytes);
    }

}
