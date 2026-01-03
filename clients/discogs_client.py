"""
Discogs Client
-------------
Handles Discogs authentication and provides a robust client.
"""
import os
import time
import logging
import re
import json
from datetime import datetime
from dotenv import load_dotenv
import discogs_client
from constants import (
    DISCOGS_RATE_LIMIT_PER_MINUTE,
    DISCOGS_MAX_RETRIES,
    DISCOGS_RETRY_AFTER_BUFFER,
)

logger = logging.getLogger('discogs_client')

class DiscogsClient:
    """
    A robust wrapper around the Discogs API client.
    
    Implements rate limiting, error handling, and useful helper methods.
    """
    
    def __init__(self, user_token=None, cache_manager=None):
        """
        Initialize the Discogs client with the user token.
        
        Args:
            user_token: Discogs API user token (defaults to env var)
            cache_manager: Optional cache manager instance
        """
        load_dotenv()
        self.user_token = user_token or os.getenv('DISCOGS_USER_TOKEN')
        
        if not self.user_token:
            # Don't raise error immediately, allow initialization without token if not used
            logger.warning("Discogs user token not provided. Some features will be disabled.")
            self.client = None
        else:
            self.client = discogs_client.Client('PlaylistCreatorApp/1.0', user_token=self.user_token)
            
        self.cache_manager = cache_manager
        
        # Initialize rate limiting parameters - more conservative defaults
        self.rate_limit_per_minute = DISCOGS_RATE_LIMIT_PER_MINUTE  # Default (Discogs recommends below 60)
        self.min_request_interval = 60.0 / self.rate_limit_per_minute  # Dynamic calculation
        self.last_request_time = 0
        
        # Token bucket for rate limiting
        self.token_bucket = self.rate_limit_per_minute  # Start with full bucket
        self.bucket_last_updated = time.time()

        # Test the connection if client exists
        if self.client:
            try:
                self.client.identity()
                logger.info("Successfully authenticated with Discogs")
            except Exception as e:
                logger.error(f"Failed to authenticate with Discogs: {e}")
                # Don't raise, just log
    
    def _update_token_bucket(self):
        """Update token bucket based on elapsed time."""
        now = time.time()
        elapsed = now - self.bucket_last_updated
        
        # Add tokens based on elapsed time
        new_tokens = elapsed * (self.rate_limit_per_minute / 60.0)
        self.token_bucket = min(self.rate_limit_per_minute, self.token_bucket + new_tokens)
        self.bucket_last_updated = now

    def _wait_for_rate_limit(self):
        """
        Implement more efficient token bucket algorithm for rate limiting.
        """
        # Update token bucket first
        self._update_token_bucket()
        
        # Smarter approach: Only wait if we're below threshold
        if self.token_bucket < 1:
            # Calculate how much time needed until we have a token
            seconds_needed = (1 - self.token_bucket) * (60.0 / self.rate_limit_per_minute)
            
            # Add a small buffer to be safe
            wait_time = seconds_needed + 0.1
            
            # Sleep for the calculated time instead of polling
            time.sleep(wait_time)
            
            # Update bucket after sleep
            self._update_token_bucket()
    
        # Consume one token
        self.token_bucket -= 1
        self.last_request_time = time.time()
    
    def _request_with_backoff(self, func, *args, max_retries=DISCOGS_MAX_RETRIES, **kwargs):
        """
        Execute a Discogs API call with exponential backoff for failures.
        """
        if not self.client:
            raise RuntimeError("Discogs client not initialized (missing token)")

        retry = 0
        while retry <= max_retries:
            try:
                # Add a longer mandatory wait between requests
                self._wait_for_rate_limit()
                result = func(*args, **kwargs)
                
                # Adjust rate limiting based on headers if available
                try:
                    if hasattr(result, '_resp'):
                        remaining = int(result._resp.headers.get('X-Discogs-Ratelimit-Remaining', 60))
                        
                        # Dynamically adjust request interval based on remaining limit
                        if remaining < 5:
                            self.min_request_interval = 5.0  # Slow down significantly
                        elif remaining < 20:
                            self.min_request_interval = 2.0  # Slow down moderately
                        else:
                            self.min_request_interval = 1.0  # Normal speed
                except (AttributeError, ValueError):
                    pass
                    
                return result
                
            except Exception as e:
                # Don't retry on 404 Not Found
                if hasattr(e, 'status_code') and e.status_code == 404:
                    raise

                retry += 1
                
                # Check for rate limit error message
                rate_limited = False
                wait_time = 0
                
                if hasattr(e, 'status_code') and e.status_code == 429:
                    rate_limited = True
                    wait_time = int(getattr(e, 'headers', {}).get('Retry-After', 60))
                elif str(e).lower().find('rate') >= 0 and str(e).lower().find('limit') >= 0:
                    rate_limited = True
                    # Try to extract the wait time from the error message
                    match = re.search(r'after:\s*(\d+)', str(e))
                    if match:
                        wait_time = int(match.group(1))
                    else:
                        wait_time = min(60, 5 * (2 ** retry))
                
                if retry <= max_retries:
                    if rate_limited:
                        # Use the provided wait time + a small buffer
                        wait_time = wait_time + 1
                        logger.warning(f"Discogs rate limit hit. Waiting {wait_time}s (retry {retry}/{max_retries})")
                    else:
                        # Use exponential backoff for other errors
                        wait_time = min(60, 2 ** retry)
                        logger.warning(f"Discogs API error: {e}. Waiting {wait_time}s (retry {retry}/{max_retries})")
                    
                    # Wait for the required time
                    time.sleep(wait_time)
                    
                    # Increase the minimum request interval
                    self.min_request_interval = max(self.min_request_interval, 1.0 + (retry * 0.5))
                else:
                    logger.error(f"Maximum retries reached for Discogs request: {e}")
                    raise
    
        raise Exception("Maximum retries reached for Discogs request")
    
    def find_label_by_url(self, url):
        """Find a Discogs label by URL."""
        try:
            # Extract label ID from URL
            match = re.search(r'/label/(\d+)', url)
            if not match:
                logger.warning(f"Could not extract label ID from URL: {url}")
                return None
                
            label_id = int(match.group(1))
            
            # Fetch the label
            label = self._request_with_backoff(lambda: self.client.label(label_id))
            logger.info(f"Found label via URL: {label.name}")
            return label
            
        except Exception as e:
            logger.error(f"Error finding label by URL {url}: {e}")
            return None
    
    def find_label_by_name(self, name):
        """Find a Discogs label by name."""
        try:
            # Search for the label
            results = self._request_with_backoff(
                lambda: self.client.search(name, type='label')
            )
            
            if not results or len(results) == 0:
                logger.warning(f"No labels found for name: {name}")
                return None
                
            # Find the best match in search results
            best_match = None
            
            for result in results:
                # Check the attribute that actually exists (discogs search results use 'title')
                result_name = None
                if hasattr(result, 'title'):
                    result_name = result.title
                elif hasattr(result, 'name'):
                    result_name = result.name
                
                if result_name and result_name.lower() == name.lower():
                    best_match = result
                    break
            
            # Use first result if no exact match found
            if not best_match and len(results) > 0:
                best_match = results[0]
                
            # If we have a match, get the full label object
            if best_match:
                try:
                    # Results contain objects with 'id' attribute - we need this to get the full label
                    label_id = best_match.id
                    label = self._request_with_backoff(lambda: self.client.label(label_id))
                    logger.info(f"Found label by name: {label.name}")
                    return label
                except AttributeError:
                    # Fall back to direct fetch if ID isn't available
                    logger.warning(f"Search result missing ID attribute, trying direct label fetch")
                    labels = self.client.search(name, type='label')
                    if labels and len(labels) > 0:
                        return self.client.label(labels[0].id)
            
            return None
                
        except Exception as e:
            logger.error(f"Error finding label by name {name}: {e}")
            return None
    
    def get_all_label_releases(self, label, cache_key=None, force_update=False):
        """
        Get all releases for a label with intelligent caching and rate limiting.
        """
        # Check cache first unless force update is specified
        if cache_key and not force_update:
            cached_data = self.cache_manager.load_from_cache(cache_key) if self.cache_manager else None
            if cached_data:
                logger.info(f"Loaded {len(cached_data)} releases from cache for {label.name}")
                return cached_data

        try:
            releases = []
            
            # Get releases with the correct pagination API
            logger.info(f"Fetching releases for {label.name} from Discogs API")
            
            # Get the releases object
            releases_obj = self._request_with_backoff(
                lambda: self.client.label(label.id).releases
            )
            
            # Configure pagination parameters on the releases object
            releases_obj.per_page = 100  # Max allowed by API
            
            # Get first page
            first_page = self._request_with_backoff(
                lambda: releases_obj.page(1)
            )
            
            # Extract pagination information
            total_pages = 1
            
            # Try all known pagination patterns in Discogs client responses
            if hasattr(first_page, 'pagination'):
                pagination = first_page.pagination
                
                if hasattr(pagination, 'pages'):
                    total_pages = pagination.pages
                elif hasattr(pagination, 'items') and pagination.items:
                    items_per_page = getattr(pagination, 'per_page', 100)
                    total_items = pagination.items
                    total_pages = (total_items + items_per_page - 1) // items_per_page
                elif hasattr(pagination, 'urls') and pagination.urls:
                    if 'last' in pagination.urls:
                        try:
                            from urllib.parse import urlparse, parse_qs
                            parsed = urlparse(pagination.urls['last'])
                            query = parse_qs(parsed.query)
                            if 'page' in query and query['page']:
                                total_pages = int(query['page'][0])
                        except Exception as e:
                            logger.warning(f"Failed to parse pagination URL: {e}")

            # If we still only have 1 page but there are clearly more items than per_page
            if total_pages == 1 and len(first_page) >= releases_obj.per_page:
                try:
                    second_page = self._request_with_backoff(
                        lambda: releases_obj.page(2)
                    )
                    if second_page and len(second_page) > 0:
                        total_pages = 2
                        for probe_page in range(3, 10):  # Try up to page 10
                            try:
                                next_page = self._request_with_backoff(
                                    lambda: releases_obj.page(probe_page)
                                )
                                if next_page and len(next_page) > 0:
                                    total_pages = probe_page
                                else:
                                    break
                            except Exception:
                                break
                        logger.debug(f"Found {total_pages} pages through probing")
                except Exception as e:
                    logger.debug(f"Second page probe failed: {e}")
        
            logger.info(f"Found {total_pages} pages of releases for {label.name}")
            
            # Process first page results
            for release in first_page:
                processed = self._process_release(release)
                if processed:
                    releases.append(processed)
        
            # Process remaining pages
            for page in range(2, total_pages + 1):
                try:
                    logger.info(f"Fetching page {page}/{total_pages}...")
                    # Use the properly configured releases object for pagination
                    page_releases = self._request_with_backoff(
                        lambda: releases_obj.page(page)
                    )
                    
                    if not page_releases:
                        logger.warning(f"No releases found on page {page}")
                        continue
                    
                    # Process releases on this page
                    for release in page_releases:
                        processed = self._process_release(release)
                        if processed:
                            releases.append(processed)
                
                    time.sleep(1.0)  # Rate limiting
                
                except Exception as e:
                    logger.warning(f"Error fetching page {page}: {e}")
                    time.sleep(5.0)  # Backoff on error
        
            logger.info(f"Found {len(releases)} releases for label {label.name}")
        
            # Cache the results
            if self.cache_manager and cache_key:
                self.cache_manager.save_to_cache(cache_key, releases)
            
            return releases
        
        except Exception as e:
            logger.error(f"Failed to fetch releases for {label.name}: {e}")
            return []
    
    def _process_release(self, release):
        """
        Process a release object from Discogs API into a clean format.
        """
        try:
            # Basic validation
            if not release or not hasattr(release, 'id'):
                return None
                
            # Extract basic release information
            release_data = {
                'id': release.id,
                'title': getattr(release, 'title', 'Unknown'),
                'artist': getattr(release, 'artist', 'Various'),
                'year': getattr(release, 'year', None),
                'format': getattr(release, 'format', 'Unknown'),
                'catno': getattr(release, 'catno', ''),
                'resource_url': getattr(release, 'resource_url', ''),
            }
            return release_data
                
        except json.JSONDecodeError as e:
            logger.warning(f"Error processing release {release.id}: {e}")
            return {
                'id': release.id,
                'title': getattr(release, 'title', 'Unknown'),
                'artist': getattr(release, 'artist', 'Various'),
            }
        except Exception as e:
            logger.warning(f"Error processing release {release.id}: {e}")
            return None

def get_discogs_client(user_token=None, cache_manager=None):
    """Create and initialize a Discogs client."""
    try:
        return DiscogsClient(user_token, cache_manager)
    except Exception as e:
        logger.error(f"Failed to initialize Discogs client: {e}")
        raise
