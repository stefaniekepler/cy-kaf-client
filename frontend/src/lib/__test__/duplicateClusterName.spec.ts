import { nextDuplicateName } from 'lib/duplicateClusterName';

describe('nextDuplicateName', () => {
  it('appends -copy when the suffixed name is free', () => {
    expect(nextDuplicateName('prod', ['prod', 'stage'])).toEqual('prod-copy');
  });

  it('appends -copy when nothing else exists', () => {
    expect(nextDuplicateName('prod', [])).toEqual('prod-copy');
  });

  it('falls back to -copy-2 when -copy is taken', () => {
    expect(nextDuplicateName('prod', ['prod', 'prod-copy'])).toEqual(
      'prod-copy-2'
    );
  });

  it('keeps incrementing while suffixed names are taken', () => {
    expect(
      nextDuplicateName('prod', [
        'prod',
        'prod-copy',
        'prod-copy-2',
        'prod-copy-3',
      ])
    ).toEqual('prod-copy-4');
  });

  it('skips gaps by taking the first free suffix', () => {
    expect(
      nextDuplicateName('prod', ['prod', 'prod-copy', 'prod-copy-3'])
    ).toEqual('prod-copy-2');
  });

  it('duplicates an already duplicated cluster', () => {
    expect(nextDuplicateName('prod-copy', ['prod', 'prod-copy'])).toEqual(
      'prod-copy-copy'
    );
  });

  it('matches names exactly and is case sensitive', () => {
    expect(nextDuplicateName('prod', ['PROD-COPY'])).toEqual('prod-copy');
  });
});
