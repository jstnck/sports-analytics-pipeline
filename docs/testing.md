# Testing Guide

## Test Structure

### Unit Tests (Fast)
- `test_ingest_transformations.py` - Pure data transformation logic
- `test_storage.py` - Database storage functionality

### Integration Tests (Fast, Mocked)
- `test_ingest_integration.py` - dlt pipeline integration with mocked APIs

### Live Integration Tests (Real API)
- `test_live_integration.py` - Real ESPN API calls for verification

## Running Tests

### Default (Fast tests only)
```bash
pytest tests/ -v
```

### Include live API tests
```bash
LIVE_TESTS=1 pytest tests/ -v -s
```

### Run specific test file
```bash
pytest tests/test_ingest_transformations.py -v
```

### Run only live tests
```bash
LIVE_TESTS=1 pytest tests/test_live_integration.py -v -s
```

### Manual API testing
```bash
python tests/test_live_integration.py
```

## Environment Variables

- `LIVE_TESTS=1` - Enable live API tests (disabled by default)

## Coverage

```bash
pytest tests/ --cov=sports_analytics_pipeline --cov-report=html
```