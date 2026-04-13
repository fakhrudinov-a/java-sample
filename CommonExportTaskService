
@Service
@RequiredArgsConstructor
public class CommonExportTaskService {

    private final ExportTaskRepository exportTaskRepository;
    private final ExportTaskProducer producer;
    private final ExportTaskSpecification exportTaskSpecification;
    private final S3FileService s3FileService;
    private final RedisRequestIdService redisRequestIdService;

    public Page<ExportTaskShortResponse> list(
            ExportTaskPageFilter filter,
            ExportDomain exportDomain,
            Long currentUserId
    ) {
        var specification = exportTaskSpecification.from(filter, exportDomain, currentUserId);
        return exportTaskRepository.findAll(specification, filter.getPageRequest())
                .map(ExportTaskConverter::toShortResponse);
    }

    public ExportTaskResponse createExportTask(
            ExportTaskFilter request,
            ExportDomain exportDomain,
            Long currentUserId
    ) {
        validateRequestIdIsUnique(request.getRequestId());

        var exportTask = buildAndSave(request.getRequestId(), exportDomain, currentUserId);
        var exportTaskDto = new ExportTaskDto<>(exportTask.getExportDomain(), request);
        producer.publish(exportTask.getRequestId().toString(), exportTaskDto);

        return new ExportTaskResponse(request.getRequestId());
    }

    public void update(ExportTask exportTask) {
        exportTaskRepository.save(exportTask);
    }

    public void failExportTask(UUID requestId) {
        updateStatusIfExists(requestId, ExportTaskStatus.FAILED);
    }

    public void markExportTaskInProgress(UUID requestId) {
        updateStatusIfExists(requestId, ExportTaskStatus.IN_PROGRESS);
    }

    public ExportTaskStatusResponse getStatusByUid(UUID requestId) {
        var exportTask = findById(requestId);
        return new ExportTaskStatusResponse(exportTask.getStatus());
    }

    public ExportTaskFileUrlResponse getFileUrlByUid(UUID requestId) {
        var exportTask = findById(requestId);
        if (exportTask.getStatus() != ExportTaskStatus.READY) {
            throw new IllegalStateException("Export task is not READY with requestId: " + requestId);
        }
        var s3FilePath = s3FileService.generatePreSignedUrl(exportTask.getS3Path(), S3Bucket.EXPORT);
        return new ExportTaskFileUrlResponse(s3FilePath);
    }

    public ExportTask findById(UUID requestId) {
        return exportTaskRepository.findById(requestId)
                .orElseThrow(() -> new NotFoundException("Export task not found: " + requestId));
    }

    private void validateRequestIdIsUnique(UUID requestId) {
        var key = buildKey(RequestIdEntityType.EXPORT_TASK, requestId);

        if (redisRequestIdService.getByKey(key).isPresent()
                || exportTaskRepository.existsById(requestId)) {
            throw new BadRequestException("Export task with requestId: %s already exists".formatted(requestId));
        }
    }

    private void updateStatusIfExists(UUID requestId, ExportTaskStatus status) {
        if (exportTaskRepository.existsById(requestId)) {
            exportTaskRepository.updateStatus(requestId, status);
        }
    }

    private ExportTask buildAndSave(
            UUID requestId,
            ExportDomain exportDomain,
            Long currentUserId
    ) {
        var exportTask = buildExportTask(requestId, exportDomain, currentUserId);
        redisRequestIdService.put(RequestIdEntityType.EXPORT_TASK, requestId);
        return exportTaskRepository.save(exportTask);
    }

    private static ExportTask buildExportTask(
            UUID requestId,
            ExportDomain exportDomain,
            Long currentUserId
    ) {
        return new ExportTask()
                .setRequestId(requestId)
                .setExportDomain(exportDomain)
                .setStatus(ExportTaskStatus.CREATED)
                .setCreatedBy(currentUserId);
    }
}
